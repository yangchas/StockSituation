# Reporting Source Closure — 2026-08-25

## Scope

This commit closes the minimum source dependencies for the existing auction
report and the OpenConfirmation pure-input boundary. It does not deploy or
enable production reporting.

## Commit

- Source baseline: `7af7f79c1372b956e8711e297413047434f5cb3c`
- Closure commit: `5b1f708`
- Worktree: independent clean worktree
- Production changes: none

## Included

- Existing `AuctionEmailReportV1` HTML template;
- Optional HTML alternative on the existing notification payload;
- Existing file-based OpenConfirmation wrapper preserved;
- New pure-input OpenConfirmation boundary using the same computation;
- Tracked closure test for live-compatible Q2 input.

## Verification

```text
python -m pytest -q \
  engine_next/tests/replay_fixture_checks.py \
  engine_next/tests/auction_email_report_checks.py \
  engine_next/tests/auction_open_confirmation_checks.py

40 passed
```

The live-input test verifies that the pure input path and existing file/replay
path produce the same open facts for the same auction shadow and Q2 frames.

## Not evaluated

- Production Ground Truth capture;
- Production auction/open fact equivalence;
- Production scheduler wiring;
- SMTP/provider delivery acceptance;
- Mailbox canary;
- Decision isolation in the production release.

## Stop condition

The closure commit must not be deployed or enabled as the production mail path
until a complete trading-day Ground Truth capture passes the production fact
gates and the reporting Shadow/Canary gates are separately verified.
