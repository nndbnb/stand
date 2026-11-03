# ЛР3/ЛР4 — WSL verification (20261103T074026Z)

- Mode: apply
- Commit: fe1fc51
- Full operational log stays under data/lr34/ (not tracked by Git).
- Retention deletion and snapshot expiry are NEVER invoked by this script.
- Historical candles are stored separately in clean/history/; they are not marketdata.
- An old LR1 restore point cannot be created retrospectively by this script.

## Checks
- Python syntax: PASS
- Compose configuration: PASS
