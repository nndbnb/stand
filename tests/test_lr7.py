import json
import sqlite3
import sys
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobs"))
sys.path.insert(0, str(ROOT / "scripts"))
import clean
import iceberg_load
import lr7
import lr7_notes


class DailyPartition(unittest.TestCase):
    def test_clean_and_load_rollover(self):
        day = date(2026, 10, 31)
        env = {"S3_BUCKET": "lake"}
        paths, output, sql = clean.prepare(str(day), env, {"2026-10-31", "2026-11-01"})
        self.assertIn("dt=2026-11-01", paths[1])
        self.assertIn("event_date=2026-10-31", output)
        self.assertIn("event_time < TIMESTAMP '2026-11-01 00:00:00'", sql)
        self.assertIn("\n)\nTO ", sql.split("COPY (")[1])
        sql = iceberg_load.render(*lr7.bounds(day), output)
        self.assertIn("BEGIN TRANSACTION;", sql)
        self.assertIn("event_time < TIMESTAMP '2026-11-01 00:00:00'", sql)
        self.assertNotIn("__", sql)
        with self.assertRaisesRegex(ValueError, "D\\+1"):
            clean.prepare(str(day), env, {"2026-10-31"})

    def test_all_group_values_and_uniqueness(self):
        one = [(10, "SBER", 4, 1, 20)]
        self.assertEqual(lr7.compare(one, one)["qty_sum"], 20)
        self.assertEqual(lr7.compare([], [])["rows"], 0)
        for changed in ([(10, "GAZP", 4, 1, 20)], [(10, "SBER", 4, 1, 19)], []):
            with self.assertRaises(ValueError):
                lr7.compare(one, changed)
        with self.assertRaisesRegex(ValueError, "повтор"):
            lr7.compare(one + one, one + one)

    def evaluate(self, last_hour=20, qty=1, inject=False):
        # Exercise the actual quality SQL over a small in-memory SQL table.
        conn = sqlite3.connect(":memory:")
        cols = "event_key, event_time, received_at, source, SECID, BOARDID, QTY, LAST"
        conn.execute(f"CREATE TABLE events ({cols})")
        conn.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?,?)",
                     ("a", f"2026-10-30 {last_hour}:00:00", "2026-10-30", "moex", "SBER", "TQBR", qty, 1))
        conn.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?,?)",
                     (None, "2026-10-31 00:00:00", None, "moex", "SBER", "TQBR", -2, 1))

        class LocalDB:
            def close(self):
                conn.close()

            def execute(self, sql, params):
                last, required, ranges = conn.execute(sql.replace("lake.clean.events", "events"),
                                                       [str(v) for v in params]).fetchone()
                self.row = (datetime.fromisoformat(last), required, ranges)
                return self

            def fetchone(self):
                return self.row

        with patch.object(lr7, "duck", LocalDB):
            return lr7.quality(date(2026, 10, 30), inject)

    def test_quality_utc_age_and_day_boundary(self):
        self.assertEqual(self.evaluate()["freshness"], "OK")
        self.assertEqual(self.evaluate(last_hour=17)["freshness"], "WARN")

    def test_quality_required_and_range(self):
        for params, key in [({"inject": True}, "missing_required"), ({"qty": -1}, "invalid_values")]:
            with self.assertRaises(ValueError) as error:
                self.evaluate(**params)
            self.assertEqual(json.loads(str(error.exception))[key], 1)

    def test_notes_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            notes = folder / "NOTES.md"
            report = folder / "report.json"
            notes.write_text("old\n<!-- lr7-aggregate -->\npending\n<!-- /lr7-aggregate -->\n")
            report.write_text(json.dumps(dict(day="D", app="A", stages=[dict(stageId=1, numCompleteTasks=1,
                inputBytes=42, shuffleReadBytes=0, shuffleWriteBytes=0)], input_bytes=42, day_files=1,
                history_files=2, day_file_bytes=100, plan="BatchScan event_time")))
            with patch.object(lr7_notes, "ROOT", folder), patch.object(sys, "argv", ["notes", str(report)]):
                lr7_notes.main()
                first = notes.read_text()
                lr7_notes.main()
            self.assertEqual(notes.read_text(), first)
            self.assertIn("42", first)
            self.assertIn("BatchScan", first)


if __name__ == "__main__":
    unittest.main()
