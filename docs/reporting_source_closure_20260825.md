# Reporting Source Closure — 2026-08-25

## Scope

This commit closes the minimum source dependencies for the existing auction
report and the OpenConfirmation pure-input boundary. It does not deploy or
enable production reporting.

## Commit

- Source baseline: `7af7f79c1372b956e8711e297413047434f5cb3c`
- Closure commits: `5b1f708`, `23dceb8`, `d6029e8`, `07f7aa4`
- Worktree: independent clean worktree
- Production changes: none

## Included

- Existing `AuctionEmailReportV1` HTML template;
- Optional HTML alternative on the existing notification payload;
- Explicit `notify_auction_report(report, request)` input boundary; notification does not read report files;
- Existing file-based OpenConfirmation wrapper preserved;
- New pure-input OpenConfirmation boundary using the same computation;
- Tracked closure test for live-compatible Q2 input.

## Verification

```text
python -m pytest -q \
  engine_next/tests/replay_fixture_checks.py \
  engine_next/tests/auction_email_report_checks.py \
  engine_next/tests/auction_open_confirmation_checks.py \
  engine_next/tests/release_provenance_checks.py

43 passed
```

The live-input test verifies that the pure input path and existing file/replay
path produce the same open facts for the same auction shadow and Q2 frames.
The notification test verifies that a caller-provided report is deduplicated
without making the generic runtime notification path read stale files.
The open-input boundary interprets a naive cutoff as Asia/Shanghai and records
the normalized observation cutoff explicitly.

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
