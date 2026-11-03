# ЛР3/ЛР4 — WSL verification (20261103T073829Z)

- Mode: check
- Commit: 0c8f76d
- Full operational log stays under data/lr34/ (not tracked by Git).
- Retention deletion and snapshot expiry are NEVER invoked by this script.
- Historical candles are stored separately in clean/history/; they are not marketdata.
- An old LR1 restore point cannot be created retrospectively by this script.

## Checks
- Python syntax: PASS
- Compose configuration: PASS
- Plan clean.events: PASS
- Plan clean.history: PASS
- Plan Iceberg loading: PASS
- S3 inventory: PASS (see inventory.json)
- LR3 raw versus clean counts: PASS
- LR3 pruning verification: PASS
- LR4 pruning verification: PASS
- Retention dry run: PASS

## Items requiring separate manual proof

- [ ] Dated LR1 restore point (not recoverable retroactively).
- [ ] External raw backup is no older than 7 days.
- [ ] Review raw/clean reconciliation output from the full local log (automatic check covers 2026-10-19 only).
- [ ] Iceberg ALL S3 object counts before/after snapshot expiration.
- [ ] LR3 bonus: cloud annual price and 1000x scenario.
- [ ] LR4 bonus: simultaneous commit conflict experiment (not run automatically).

## Publish sanitized results

    git add docs/evidence/
    git commit -m "test: lr34 WSL acceptance results"
    git push personal lab3-lab4
