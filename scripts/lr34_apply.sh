#!/usr/bin/env bash
# ЛР3/ЛР4. --check (default): verify only. --apply: external raw backup, reclean, reload.
set -Eeuo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
MODE=check
BACKUP_DIR=""
while (($#)); do
  case "$1" in
    --check) MODE=check; shift ;;
    --apply) MODE=apply; shift ;;
    --backup-dir) BACKUP_DIR="$2"; shift 2 ;;
    *) echo "Usage: bash scripts/lr34_apply.sh [--check | --apply --backup-dir /mnt/c/...]" >&2; exit 2 ;;
  esac
done
[[ "$(git branch --show-current)" == "lab3-lab4" ]] || { echo "ERROR: switch to lab3-lab4 branch" >&2; exit 2; }
[[ -f .env ]] || { echo "ERROR: missing .env" >&2; exit 2; }
command -v docker >/dev/null || { echo "ERROR: no Docker" >&2; exit 2; }
python3 -c 'import boto3' 2>/dev/null || { echo "ERROR: install boto3 in current Python (activate .venv)" >&2; exit 2; }
if [[ "$MODE" == apply ]]; then
  [[ "$BACKUP_DIR" == /mnt/[a-z]/* ]] || { echo "ERROR: --backup-dir must be outside WSL, under /mnt/c/..." >&2; exit 2; }
  [[ -d data/raw ]] || { echo "ERROR: data/raw absent" >&2; exit 2; }
  mkdir -p "$BACKUP_DIR"
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
REPORT="docs/evidence/lr34-$STAMP"
mkdir -p "$REPORT" data/lr34
LOG="data/lr34/$STAMP.log"
SUMMARY="$REPORT/summary.md"
: >"$LOG"
STATUS=0
log(){ printf '%s\n' "$*" | tee -a "$LOG"; }
run(){
  local name="$1"; shift
  log "START $name"
  if "$@" >>"$LOG" 2>&1; then
    log "PASS $name"; printf -- '- %s: PASS\n' "$name" >>"$SUMMARY"
  else
    local rc=$?
    log "FAIL $name exit=$rc"
    printf -- '- %s: FAIL (%s)\n' "$name" "$rc" >>"$SUMMARY"
    STATUS=1
    return "$rc"
  fi
}

cat >"$SUMMARY" <<EOF
# ЛР3/ЛР4 — WSL verification ($STAMP)

- Mode: $MODE
- Commit: $(git rev-parse --short HEAD)
- Full operational log stays under data/lr34/ (not tracked by Git).
- Retention deletion and snapshot expiry are NEVER invoked by this script.
- Historical candles are stored separately in clean/history/; they are not marketdata.
- An old LR1 restore point cannot be created retrospectively by this script.

## Checks
EOF

run "Python syntax" python3 -m py_compile jobs/clean.py jobs/clean_history.py jobs/lr34_inventory.py jobs/verify_clean_counts.py jobs/iceberg_load.py || exit 1
run "Compose configuration" docker compose config --quiet || exit 1
if [[ "$MODE" == apply ]]; then
  DEST="$BACKUP_DIR/lr34-$STAMP"
  mkdir -p "$DEST"
  log "Copy raw outside WSL into $DEST/raw"
  cp -a data/raw "$DEST/raw" >>"$LOG" 2>&1 || { log "BACKUP FAILURE: refusing any changes"; exit 1; }
  printf -- '- External raw backup: %s/raw\n' "$DEST" >>"$SUMMARY"
  run "Start lab4 containers" docker compose --profile lab4 up -d || exit 1
  run "Audit raw files to S3" docker compose run --rm -T --no-deps --entrypoint python collector /app/src/lake_audit.py || exit 1
  run "Rebuild clean.events eligible dates" python3 jobs/clean.py --all || exit 1
  run "Build clean.history candles" python3 jobs/clean_history.py --all || exit 1
  run "Reload Iceberg from updated clean.events" python3 jobs/iceberg_load.py --all || exit 1
else
  run "Plan clean.events" python3 jobs/clean.py --all --dry-run || true
  run "Plan clean.history" python3 jobs/clean_history.py --all --dry-run || true
  run "Plan Iceberg loading" python3 jobs/iceberg_load.py --all --dry-run || true
fi
log "START S3 inventory"
if python3 jobs/lr34_inventory.py >"$REPORT/inventory.json" 2>>"$LOG"; then
  log "PASS S3 inventory"
  echo "- S3 inventory: PASS (see inventory.json)" >>"$SUMMARY"
else
  log "FAIL S3 inventory"
  echo "- S3 inventory: FAIL" >>"$SUMMARY"
  rm -f "$REPORT/inventory.json"
  STATUS=1
fi
run "LR3 raw versus clean counts" python3 jobs/verify_clean_counts.py --day 2026-10-19 || true
run "LR3 pruning verification" python3 jobs/verify_clean.py --day 2026-10-19 || true
run "LR4 pruning verification" bash -c 'docker compose run --rm -T duckdb -init init.sql -bail -f /dev/stdin < jobs/iceberg_benchmark.sql' || true
run "Retention dry run" python3 jobs/retention.py --layer clean --days 2 --dry-run || true

cat >>"$SUMMARY" <<'EOF'

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
EOF
log "REPORT=$REPORT"
log "FULL_LOG=$LOG (ignored by Git)"
log "STATUS=$STATUS"
echo "Push results: git add docs/evidence/ && git commit -m 'test: lr34 WSL acceptance results' && git push personal lab3-lab4"
exit "$STATUS"
