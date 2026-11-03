#!/usr/bin/env bash
# ЛР3/ЛР4: read-only diagnostics and automatic sanitized Git push.
# Run: bash scripts/lr34_collect_push.sh
set -Eeuo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
BRANCH="lab3-lab4"
REMOTE="personal"
DAY="$(printenv LR34_CHECK_DAY || echo 2026-10-19)"
die(){ echo "ERROR: $*" >&2; exit 1; }
[[ "$(git branch --show-current)" == "$BRANCH" ]] || die "switch to $BRANCH first"
git remote get-url "$REMOTE" >/dev/null 2>&1 || die "missing remote $REMOTE"
[[ -f .env ]] || die "missing .env"
command -v python3 >/dev/null || die "python3 unavailable"
command -v docker >/dev/null || die "docker unavailable"
python3 -c 'import boto3' >/dev/null 2>&1 || die "boto3 missing: activate .venv"
[[ -z "$(git diff --name-only)" ]] || die "unstaged changes exist; inspect git status"
[[ -z "$(git diff --cached --name-only)" ]] || die "staged changes exist; inspect git status"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="docs/evidence/lr34-followup-$STAMP"
LOGDIR="data/lr34"
mkdir -p "$OUT" "$LOGDIR"
PLANLOG="$LOGDIR/followup-$STAMP-plan.log"
COUNTLOG="$LOGDIR/followup-$STAMP-counts.log"
PLANERR="$LOGDIR/followup-$STAMP-plan.err"
COUNTERR="$LOGDIR/followup-$STAMP-counts.err"

cat > "$OUT/summary.md" <<EOF
# ЛР3/ЛР4 — повторная проверка $STAMP

- Ветка: $BRANCH
- Коммит исходного кода: $(git rev-parse --short HEAD)
- Режим: только чтение. SeaweedFS и Iceberg не изменялись.
- Контрольная дата: $DAY
- Полные логи локально в data/lr34/ и не публикуются.

## Команды и статусы
EOF
ok=1
echo "Checking S3 source-day readiness..."
if python3 jobs/clean.py --all --dry-run >"$PLANLOG" 2>"$PLANERR"; then
  echo "- clean.py --all --dry-run: PASS" >> "$OUT/summary.md"
else
  echo "- clean.py --all --dry-run: FAIL (see local log)" >> "$OUT/summary.md"
  ok=0
fi
# Only dates and summary counters, not URLs or secrets.
grep -E '^SKIP [0-9]{4}-[0-9]{2}-[0-9]{2}:|^SUMMARY days=' "$PLANLOG" >"$OUT/skipped-days.txt" || true
[[ -s "$OUT/skipped-days.txt" ]] || echo "No SKIP/SUMMARY lines captured; inspect local plan log." >"$OUT/skipped-days.txt"

echo "Checking raw->clean aggregate counts for $DAY..."
if python3 jobs/verify_clean_counts.py --day "$DAY" >"$COUNTLOG" 2>"$COUNTERR"; then
  echo "- verify_clean_counts.py --day $DAY: PASS" >> "$OUT/summary.md"
  # Retain only numeric SQL table cells / known field names; no arbitrary logs.
  python3 - "$COUNTLOG" "$OUT/raw-clean-counts.txt" <<'PY'
import re
import sys
from pathlib import Path
heads = re.compile(r'(input_raw_D_and_D1|event_day_rows|invalid_or_empty|eligible_rows|expected_unique_keys|actual_clean_rows|actual_clean_unique_keys)')
numeric = re.compile(r'^[\s|│┌┐└┘├┤┬┴┼─┏┓┗┛┣┫┳┻╋+,\d.-]+$')
lines = Path(sys.argv[1]).read_text(encoding='utf-8', errors='replace').splitlines()
selected = [line for line in lines if heads.search(line) or (numeric.fullmatch(line) and re.search(r'\d', line))]
Path(sys.argv[2]).write_text('\n'.join(selected) + '\n' if selected else 'No sanitized numeric rows; inspect local count log.\n', encoding='utf-8')
PY
else
  echo "- verify_clean_counts.py --day $DAY: FAIL (see local log)" >> "$OUT/summary.md"
  echo "Numeric verification failed; see local log." >"$OUT/raw-clean-counts.txt"
  ok=0
fi

cat >>"$OUT/summary.md" <<'EOF'

## Ограничения
- PASS означает успешную работу команды, но не доказывает полноту всех часов.
- Сверка сделана для одной даты, не для всего календарного архива.
- Исторические точки восстановления и прошлые S3-снимки не восстановлены.
- Дополнительные задания ЛР3/ЛР4 этим скриптом не выполняются.
EOF

echo "Commit and push: $OUT"
git add -- "$OUT/summary.md" "$OUT/skipped-days.txt" "$OUT/raw-clean-counts.txt"
git diff --cached --quiet && die "nothing staged"
# No additional paths may be staged.
if git diff --cached --name-only | grep -v "^$OUT/" >/dev/null; then
  die "unexpected staged files; refusing to commit"
fi
git commit -m "test(lr34): publish readiness and count checks $STAMP"
if ! git push "$REMOTE" "$BRANCH"; then
  echo "Push failed. Local commit is kept. Retry: git push $REMOTE $BRANCH" >&2
  exit 1
fi
echo "PUBLISHED: https://github.com/nndbnb/stand/tree/$BRANCH/$OUT"
if [[ "$ok" -ne 1 ]]; then
  echo "One or more diagnostics failed; results are published for review." >&2
  exit 2
fi
