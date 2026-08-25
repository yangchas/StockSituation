# Production Runtime Integration — implementation status

本次修改基于 `dbd784b338700370928d4aeb4fa8bcb4b9625fb3` 的独立 clean worktree，未触碰根工作树的 unrelated dirty changes。

## 已实现并通过本地验证

- 只读 `ProductionAuctionFacts` 组装：Redis 09:25 A2 summary + TD `auction_snapshot_v2` 0920/0924/0925 + 同日 `market:stock_plate`。
- TD 行字段只做单位/字段归一化，不复制 AuctionCalculator 公式。
- 运行时 mapping snapshot：按交易日原子写入、重启复用、交易日和 SHA-256 校验。
- `build_open_confirmation_observation()` 兼容入口，内部仍调用既有纯变换。
- 09:32 scheduler event 的最小生命周期切口；没有注入 reporting coordinator 时 fail-closed，不发送邮件。
- 开盘事实通知复用现有 `RuntimeNotificationService`，应用层 dedup 仍按 `trade_date + category`。
- reporting lifecycle helper：recovery/manual/historical 默认不可发送。

本地验证：`56 passed`，并通过 `compileall` 与 `git diff --check`。

## 尚未宣称通过的 Gate

以下 Gate 仍需在 cobra-ion 真实交易日证据上完成，当前不得部署新邮件：

- `RELEASE_PROVENANCE_PASS`
- `GROUND_TRUTH_CAPTURE_COMPLETE`
- `PRODUCTION_FACT_TAKEOVER_PASS`
- `PRODUCTION_RUNTIME_INTEGRATION_PASS`
- `PRODUCTION_REPORTING_PASS`
- `PRODUCTION_EMAIL_TAKEOVER_PASS`
- `decision_isolation`

特别是 effective auction universe 仍标记为 `candidate_pending_ground_truth`，没有把 `am/br/ar > 0` 擅自冻结成生产合同。

## 当前生产审计结论

只读检查目标机器为 `cobra-ion`（SSH broker 8878；本次 broker 探针挂起后使用一次性只读 SSH 复核）。当前生产 release 仍为旧版本，运行中的 t1 二进制为临时路径 artifact；2026-08-25 16:36（Asia/Shanghai）复核未发现 `market:auction*` 事实键，也没有已部署的新 Auction/Open report source。因此本次只完成本地闭包和 fail-closed 接线，未进行远程写入、重启或邮件发送。

## 下一停止线

先在 Release A 完成真实 provenance 与完整 Ground Truth capture；逐字段 equivalence 通过后，才允许注入 coordinator、启用 Release B 的 09:26/09:32 reporting，并分别完成 Shadow、Canary、Takeover。
