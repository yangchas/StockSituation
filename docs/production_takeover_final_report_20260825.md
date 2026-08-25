# Production Takeover Final Report — 2026-08-25

## Scope and host

- Production identity: `cobra-ion.exe.xyz`
- Production runtime commit observed: `eeb64e658136d63853e9597c9a8d1e21b715953b`
- Engine release observed: `/home/exedev/services/engine-next/releases/20260801_172200`
- Existing t1 service: `t1-v2-live.service`
- This report contains no credentials and no production writes were performed.

## Current execution branch

- Isolated source-closure branch: `codex/production-fact-takeover`
- Latest local commit: `4fd930e1d0699e968c3ebbf26f2ec2fab1a9aab1`
- Reporting closure verification: 50 targeted tests passed
- Production deployment: not performed

The branch closes the report/open-input source boundary, accepts production-
shaped online Q2 rows, adds a fact-only plate-shadow assembler, and tightens
release provenance checks. It does not replace the frozen replay baseline,
deploy to production, or certify production fact equivalence.

## Captures

### TDengine table audit

The production connection was read-only against `market_data1` on `cobra-ion`.
The schema audit found:

- `stock_tick_v2` is the persisted raw tick supertable.
- `auction_snapshot_v2` is the persisted auction snapshot supertable with `px_milli`, `chg_bp`, `match_amt_yuan`, `rest_bid_amt_yuan`, `rest_ask_amt_yuan`, and `limit_state`.
- `auction_summary_v2` is the persisted market auction summary with three rows for `20260824` (0920/0924/0925).
- Per-symbol `a2_YYYYMMDD_HHMM_<symbol>` tables exist for `20260824` only: 5,210 tables for each of 0920, 0924 and 0925.
- There are no `q2` or `stock_tick`-named TD tables; continuous ticks are stored in `stock_tick_v2`.
- For `20260825`, `auction_summary_v2` has 0 rows and `auction_snapshot_v2` has 0 rows; no current-day `a2_*` group was present.

The production tick funnel was nevertheless present in `stock_tick_v2`:

| Window | Rows | Distinct symbols | observed timestamp range |
|---|---:|---:|---|
| 2026-08-25 09:15–09:26 | 218,338 | 5,212 | 09:15:00–09:25:04 |
| 2026-08-25 09:30–09:32:59 | 305,002 | 5,208 | 09:30:00–09:32:59 |
| 2026-08-24 09:15–09:26 | 218,639 | 5,210 | 09:15:00–09:25:04 |
| 2026-08-24 09:30–09:32:59 | 305,893 | 5,207 | 09:30:00–09:32:59 |

The absence of current-day auction rows is a persistence/capture gap, not evidence that TDengine is empty.

### 09:25 auction context

- Read-only snapshot: `/tmp/production_capture_20260825/context_0925_latest.json`
- Snapshot business SHA-256: `e3fb36238e22c7ea5040766dc146b936877b68e1c66b0116ea47ce182207b302`
- Available source: `market:auction:20260825:latest`, observed at 09:25:04
- Latest summary: 5,204 stocks, 14,533,096,457 yuan auction amount, 5 limit-up, 1 limit-down
- Missing: `0920`, `0924`, `0925` keys and `market:auction:anchor:20260825`
- Mapping: current cache only; no historical source date was captured
- Snapshot status: incomplete (9/15 required keys)

The Redis `latest` snapshot is not substituted for the missing three anchors. It is recorded only as a current-day partial fact.

### 09:30–09:32 open window

- TD source: `market_data1.stock_tick_v2`
- TickPack SHA-256: `08d62e725d919b32f0c2e21a93f81bd7fdbdccd1b3368d12f443610a97145232`
- Rows: 305,002; symbols: 5,208; source reject: 0
- Time range: 09:30:00–09:32:59
- Reconstruction completed with the replay baseline, but it is a reconstruction, not online ground truth.

The read-only Redis Q2 capture was taken at 09:33:22:

- Q2 capture SHA-256: `016f1e0dc85b6181dd3e8a4924010ad1de9bb999f8ad8b86c69aa1b0801b3e4a`
- Rows/symbols: 5,214 / 5,214
- Time range in captured `ts`: midnight stale rows through 09:33:21
- It does not preserve the 09:30–09:32 online history and therefore is not accepted as the open-window ground truth.

## Production fact gates

| Gate | Result | Reason |
|---|---|---|
| online_auction_equivalence | NOT_EVALUATED | 0920/0924/0925/anchor production facts were not persisted |
| auction_universe | PARTIAL | latest exists, anchor universe does not |
| online_px_pc_equivalence | NOT_EVALUATED | no sealed 09:30–09:32 online Q2 history |
| online_amt2m_equivalence | PARTIAL | no sealed online `amt2m` ground truth |
| online_ls_equivalence | PARTIAL | no sealed online `ls` plus ST/limit metadata |
| st_limit_metadata_equivalence | NOT_EVALUATED | same-day metadata was not sealed |
| auction_fact_takeover | NOT_EVALUATED | fact equivalence gate not passed |
| open_fact_takeover | NOT_EVALUATED | fact equivalence gate not passed |

Therefore: `PRODUCTION_FACT_TAKEOVER_PARTIAL`.

## Production delivery audit

The current production release does not contain the local `auction_email_report.py`, auction report template, or the replay/open confirmation report path. Existing runtime scheduling and notification code is present, but a production-linked 09:26/09:32 report delivery path was not proven in this run. The service unit runs with a 180-second loop interval; exact report-slot wake-up was not accepted without a scheduler evidence run.

The current release directory also has no `.git` metadata and no `build_info.json`; release provenance is therefore `NOT_EVALUATED`, not inferred from a claimed commit. A read-only check on 2026-08-25 found zero `market:auction:20260825:*` Redis keys and no production report template. No production files or data were modified.

| Gate | Result |
|---|---|
| auction_scheduler | NOT_EVALUATED |
| auction_report_generation | NOT_EVALUATED |
| auction_notification_acceptance | NOT_EVALUATED |
| auction_mailbox_canary | NOT_EVALUATED |
| open_scheduler | NOT_EVALUATED |
| open_report_generation | NOT_EVALUATED |
| open_notification_acceptance | NOT_EVALUATED |
| open_mailbox_canary | NOT_EVALUATED |
| decision_isolation | NOT_EVALUATED |

Therefore: `PRODUCTION_EMAIL_TAKEOVER_PARTIAL`.

The application-level report notification API is now available in the isolated
reporting closure branch, but it has not been deployed to this release and is
not a production delivery result.

## Final status

`PRODUCTION_TAKEOVER_PARTIAL`.

The replay baseline is formally reproducible. Production takeover is not declared because the production side did not retain the required auction anchors and open-window online Q2 facts, and the current release has not yet proven the two email delivery gates.

## Required next capture

Before another equivalence attempt, one complete trading day must seal, without substitution:

1. 09:10 mapping, ST and price-limit metadata;
2. 09:20, 09:24, 09:25 and anchor auction facts;
3. online 09:30–09:32 `px/pc/amt2m/ls` facts or an existing historical store containing those exact observations;
4. one production scheduler run for 09:26 and 09:32 with report hash, notification result and mailbox canary.

No strategy, Candidate, Score, position, EngineCore, Q2 contract or TD schema change is authorized by this report.
