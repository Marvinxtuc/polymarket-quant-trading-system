# Polymarket Personal Decision Terminal

A pragmatic, configurable personal trading terminal for Polymarket.

Default mode is `paper trading` + `manual` decision review. The system now queues wallet-follow opportunities as candidates first, then lets you `ignore / watch / buy_small / follow` before execution. It can still be switched to live CLOB execution once credentials are configured.

## What It Does

- Polls target wallets from `WATCH_WALLETS`
- Detects new/increased positions from wallet events with position-diff fallback
- Enriches them into a candidate queue with wallet score, spread/chase context, and suggested action
- Adds 5m / 30m momentum context from `prices-history` so candidates show trend instead of only current spread
- Persists candidates, wallet profiles, and journal entries in local SQLite
- Executes via `PaperBroker` (default) or `LiveClobBroker` (optional, needs `py-clob-client` + key config) after decision-mode gating

## Strategy (Candidate-first)

Candidate + signal flow:
- Primary path: read wallet `/trades` and/or `/activity` events (`WALLET_SIGNAL_SOURCE=trades|activity|hybrid`) and emit follow signals from fresh wallet trade events.
- Fallback path: if event data is missing or delayed, use active-position diffs so the bot still sees adds / trims / full exits.
- Warm-up behavior remains conservative: the first cycle primes event cursors and current wallet state, instead of replaying already-open exposure.
- Strategy internals now split into `detect_wallet_events() -> build_candidates() -> rank_candidates() -> generate_signals()`, so the UI can show why an idea is worth looking at before it becomes an execution signal.

Wallet screening (Polymarket-native):
- Candidate wallets come from static seed list `WATCH_WALLETS` plus optional dynamic discovery from `WALLET_DISCOVERY_PATHS`.
- Discovery controls: `WALLET_DISCOVERY_ENABLED`, `WALLET_DISCOVERY_MODE`, `WALLET_DISCOVERY_PATHS` (recommend `/trades`), `WALLET_DISCOVERY_TOP_N`, `WALLET_DISCOVERY_MIN_EVENTS`, `WALLET_DISCOVERY_REFRESH_SECONDS`.
- Event-signal controls: `WALLET_SIGNAL_SOURCE`, `WALLET_SIGNAL_LOOKBACK_SECONDS`, `WALLET_SIGNAL_PAGE_SIZE`, `WALLET_SIGNAL_MAX_PAGES`.
- A wallet is monitored only if current Polymarket active positions pass all filters: `MIN_WALLET_ACTIVE_POSITIONS`, `MIN_WALLET_UNIQUE_MARKETS`, `MIN_WALLET_TOTAL_NOTIONAL_USD`, `MAX_WALLET_TOP_MARKET_SHARE`.

BR-001 repeat-entry policy (restricted live rehearsal default):
- `token_id` is the only local duplicate-entry key. Repeat-entry / add checks do not mix `market_slug` or `condition_id`.
- Local repeat-entry truth comes from the runtime `positions_book` only. Candidate cache, approved queue state, or transient account snapshots are not allowed to override that truth.
- If a local position for the same `token_id` already exists, a new BUY is blocked by default.
- Same-wallet add is allowed only when all three are true at once:
  - signal wallet equals the current position `entry_wallet`
  - `SAME_WALLET_ADD_ENABLED=true`
  - the wallet is present in `SAME_WALLET_ADD_ALLOWLIST`
- Buy-side multi-wallet resonance is now observe-only. It may enrich explanation text, but it cannot change entry notional, candidate action level, or BUY sizing tendency.
- Blocked repeat-entry candidates/export records expose both `block_reason` and `block_layer` so `/api/candidates` and `/api/state` reviews can show where the block happened.

BR-002 candidate lifecycle policy (restricted live rehearsal default):
- Candidate lifetime is enforced from one timestamp only: candidate `created_ts`.
- The active lifetime window is controlled by `CANDIDATE_TTL_SECONDS` and capped by market end when the market window is shorter.
- Once a candidate expires, it is discarded and cannot continue to influence decision review, approved queue, or execution.
- `execution-precheck` now re-checks candidate freshness before any execution path, so stale queue/manual/approved paths cannot bypass lifecycle blocking.
- Candidate exports and `/api/state` now surface lifecycle block metadata and `/metrics` exposes lifetime-expiration counts for audit/replay use.

Risk checks:
- Max risk per trade (`RISK_PER_TRADE_PCT`)
- Daily loss cap (`DAILY_MAX_LOSS_PCT`)
- Max open positions
- Condition-level portfolio netting cap (`PORTFOLIO_NETTING_ENABLED`, `MAX_CONDITION_EXPOSURE_PCT`)
- Price band guard (`MIN_PRICE` ~ `MAX_PRICE`)

Decision terminal:
- `DECISION_MODE=manual|semi_auto|auto`
- Candidate queue persisted in `CANDIDATE_DB_PATH`
- Candidate actions + journal exposed via `/api/candidates`, `/api/candidate/action`, `/api/journal`, `/api/wallet-profiles`, `/api/mode`
- Runtime stats / archive / export endpoints: `/api/stats`, `/api/archive`, `/api/export`
- Optional A-grade notifications via `CANDIDATE_NOTIFICATION_*`, `NOTIFY_WEBHOOK_URL(S)`, and Telegram Bot settings
- Frontend now includes candidate detail, notifier summary, and archive/export panels

## Quick Start

1. Create virtual env and install:

```bash
cd ~/Desktop/Polymarket
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Optional: use Makefile shortcuts:

```bash
make venv
make install
make test
```

2. Configure env:

```bash
cp .env.example .env
# edit .env
```

Optional env check:

```bash
python scripts/check_env.py
make env-check
```

3. Run one cycle (recommended first):

```bash
polybot --once
```

Or via Makefile:

```bash
make run-once
```

4. Run continuous:

```bash
polybot
```

Or via Makefile:

```bash
make run
```

5. Run tests and runtime verification:

```bash
make test
make verify-stack   # checks an already-running local stack
make verify         # env-check + tests + stack verification
make one-click      # network + tests + restart stack + runtime verify
make network-smoke  # check polymarket site/data-api/clob endpoint accessibility and geo-block hints
```

The network smoke test also writes a JSONL record by default to `/tmp/poly_network_smoke.jsonl` (set by `NETWORK_SMOKE_LOG` env var). Live startup self-checks now read the latest record from that log, so a fresh `make network-smoke` is part of the live handoff path rather than a one-off manual probe.

Before going live, run the readiness checklist:

```bash
cat preprod_readiness_checklist.md
```

For first 4h / 12h live monitoring and rollback thresholds:

```bash
cat preprod_operations_playbook.md
```

For a one-page 12h quick post-run template:

```bash
cat preprod_operations_playbook.md | sed -n '/## 七、12h 极简复盘（快速版）/,/$/p'
```

Quick stop/restart helper:

```bash
make stop-stack
```

## BlockBeats Integration

This workspace now includes a shared `blockbeats-skill` installation for live crypto and prediction-market news context.

- Shared skill source: `~/Desktop/skills/blockbeats-skill`
- Codex mirror: `~/.codex/skills/blockbeats-skill`
- OpenClaw shared skill root: `~/Desktop/skills` via `~/.openclaw/openclaw.json`
- Project helper script: `scripts/blockbeats_query.sh`

Use it when a Polymarket view depends on fresh news catalysts rather than only wallet flow or price action.

`scripts/blockbeats_query.sh` now reads `BLOCKBEATS_API_KEY` from the current shell first and falls back to this repo's `.env`, so the helper can run as soon as the key is added locally.
It also uses bounded request timeouts, retries via DNS-over-HTTPS when local DNS is unhealthy, and falls back to the public BlockBeats flash feed for `prediction` if the Pro endpoint is unreachable.

Examples:

```bash
# Quick market overview
bash scripts/blockbeats_query.sh overview

# Prediction-market specific headlines
bash scripts/blockbeats_query.sh prediction 1 10 en

# Search a named catalyst or entity
bash scripts/blockbeats_query.sh search "Trump tariffs" 1 8 en

# Pull important headlines
bash scripts/blockbeats_query.sh newsflash important 1 10 en
```

Recommended workflow for news-driven market review:

1. Pull `prediction` headlines first.
2. Add `important` and `macro` when the market is broad or policy-sensitive.
3. Run `search` for the exact person, protocol, ETF, or event.
4. Treat the result as context for thesis updates, not as a trading signal by itself.

If Codex or OpenClaw still cannot invoke the shared skill after you add the key, start a new session so the updated environment is visible to that app process.

## Dashboard + Bot (One-Click)

- Frontend: `frontend/` (this repo)
- Runtime API: `GET /api/state` served by `polymarket_bot.web`
- Decision / export APIs: `GET /api/candidates`, `GET /api/stats`, `GET /api/archive`, `GET /api/export`
- Bot daemon: `polymarket_bot.daemon` writes runtime state under `~/.local/share/poly_runtime_data/<paper|live>/<wallet>/state.json` by default; set `RUNTIME_ROOT_PATH` / `STATE_STORE_PATH` if you need a different namespace.
- Pre-production readiness checklist: `preprod_readiness_checklist.md`

Desktop launcher:
- `一键 poly.app` is intended to start this repository through the SOP entry `make one-click`
  (`network-smoke` + `run unit tests` + `start stack` + `verify stack`), then opens
  `http://127.0.0.1:8787`.
- If `一键 poly.app` still has no visible reaction on your machine, run this fallback
  (double-click friendly) shortcut:
  ```bash
  make desktop-command
  ```
  It creates `一键 poly.command` (default under `~/Desktop`, or `POLY_DESKTOP_DIR` if set) which invokes
  `scripts/run_one_click_launcher.sh` directly.
- The GUI command has on-screen output, so you can see why it exits:
  - success: prints startup message then opens dashboard
  - failure: prints the latest error lines and waits for Enter before closing.
- If current network is geo-blocked, use no-network mode:
  ```bash
  POLY_SKIP_NETWORK_SMOKE=1 make desktop-command
  ```
  This generates the same launcher shortcut but marks it to skip the smoke check step at runtime.
- If you still need `.app` to be rewritten after system cache/path changes, run:
  ```bash
  make repair-desktop-launcher
  ```
  This regenerates:
  - `~/Desktop/一键 poly.app/Contents/MacOS/start_app`
  - `~/Desktop/一键 poly.command`
- Web UI is on `http://127.0.0.1:8787`.
- `make start-stack` restarts the local stack and now verifies that `/api/state` is fresh.
- `make full-validate` now runs a quick end-to-end acceptance pass: restart stack, regenerate 30m/12h monitor JSON in `0s` quick mode, refresh EOD reconciliation through `POST /api/operator`, call the three report APIs, and run replay + replay-calibration against current runtime artifacts.
- Git autosync is available for local development: `make git-autosync-start` runs a polling watcher that automatically `git add -A`, `git commit`, and `git push origin HEAD` after file changes settle; `make git-autosync-install` installs the same watcher under `launchd`.
- Safety default: autosync refuses to start on a dirty worktree unless you explicitly set `GIT_AUTOSYNC_ALLOW_DIRTY_START=1`, so you do not accidentally auto-push the entire current worktree.
- Runtime artifacts live under `/tmp/poly_git_autosync/`, and `make git-autosync-status` shows the current watcher status plus the last sync result.

## Live Trading Enablement

1. Install live dependency:

```bash
pip install -e '.[live]'
```

2. Set in `.env`:
- `DRY_RUN=false`
- `CLOB_SIGNATURE_TYPE=0` for standard EOA wallets (`1` for Magic/email wallets, `2` for proxy/browser wallets)
- `FUNDER_ADDRESS=...`
- `SIGNER_URL=...`
- `CLOB_API_KEY=...`
- `CLOB_API_SECRET=...`
- `CLOB_API_PASSPHRASE=...`
- keep `PRIVATE_KEY` empty in live mode (raw key is fail-closed)
- Optional user stream tuning: `USER_STREAM_ENABLED=true`, `USER_STREAM_URL=wss://ws-subscriptions-clob.polymarket.com/ws/user`
- Live admission gate: set `LIVE_ALLOWANCE_READY=true`, `LIVE_GEOBLOCK_READY=true`, and `LIVE_ACCOUNT_READY=true` only after you have manually confirmed those preconditions; `LIVE_NETWORK_SMOKE_MAX_AGE_SECONDS` controls how old the latest smoke log may be before startup blocks.

3. Start bot.

Live execution note:
- `LiveClobBroker` now treats posted/live orders as `pending` until broker position reconciliation confirms the fill, instead of immediately booking them as fully filled in memory.
- Before submitting a live order, the broker now fetches CLOB `book` + `midpoint`, rounds price to valid tick size, avoids crossing the opposite quote for resting GTC orders, and rejects orders below `min_order_size`.
- Live startup gate now treats allowance/geoblock/account readiness as explicit pass/fail confirmations instead of warnings, so missing acknowledgements block `startup_ready`.
- Wallet-follow signals now default to `hybrid` mode, which prefers `/trades` / `/activity` events and only falls back to position diffs when event data is unavailable.
- Signal, pending-order, and runtime position records now carry `condition_id`, and buy-side condition-level portfolio netting is enabled by default so repeated wallet signals on the same event share one exposure budget.
- Runtime now keeps an append-only ledger at `LEDGER_PATH`, restores same-day realized PnL from that ledger on restart, and stores per-position `cost_basis_notional` in runtime state so sell-side realized PnL can survive restarts.
- Runtime recovery truth now comes from SQLite (`STATE_STORE_PATH`), not `/tmp` snapshots. `state.json` / `control.json` are export artifacts only.
- Pending truth is derived from persisted `order_intents` non-terminal states; startup recovery no longer treats file `pending_orders` as a second source of truth.
- Startup recovery conflict policy is fail-closed: unresolved `pending/position/control` conflicts latch `reduce-only` and block all BUY paths.
- In live mode, the bot also polls Polymarket accounting snapshot + current-day closed positions (`ACCOUNT_SYNC_REFRESH_SECONDS`) to surface `equity`, `cash_balance`, `positions_value`, and a conservative `broker_closed_pnl_today` risk floor.
- On startup, live mode will also try to recover still-open exchange orders from the broker so pending state is rebuilt from exchange truth before falling back to the last runtime snapshot.
- Runtime keeps a persisted SQLite state store (`STATE_STORE_PATH`) for control/runtime snapshots and idempotency keys; each process also takes a per-scope file lock (`WALLET_LOCK_PATH`) to enforce single-writer semantics across executors.
- Writer scope rule is deterministic: `<mode>:<identity>` where live uses `FUNDER_ADDRESS` and paper uses the first `WATCH_WALLETS` entry (or `default`).
- Lock conflicts now exit with fixed code `42` and `reason_code=single_writer_conflict` to distinguish from generic startup failures.
- Every state-store mutation is guarded by runtime ownership checks (`assert_writer_active`), so a process that loses ownership cannot continue advancing order/control/risk/reconciliation writes.
- Web instances without active writer ownership run strict read-only mode: write APIs return `503`, and GET endpoints avoid export-side effects (no public-state sync / lazy write paths).
- Order idempotency now uses persisted intent claim (`claim_or_load_intent`) with deterministic `idempotency_key` and stable `strategy_order_uuid`; the in-process TTL cache is advisory/rate-limit only.
- ACK_UNKNOWN recovery is bounded by `ACK_UNKNOWN_RECOVERY_WINDOW_SECONDS` and `ACK_UNKNOWN_MAX_PROBES`; over-limit intents become `manual_required` and cannot auto-create a second BUY intent.
- Pending live orders now reconcile against both broker order status and recent authenticated trade fills, so partial fills can update runtime positions without waiting for the next positions snapshot to fully catch up.
- Live execution events now also preserve structured preflight market context (`best_bid`, `best_ask`, `midpoint`, `tick_size`, `market_spread_bps`) so replay and shadow analysis can use real sample spreads.
- Live broker now exposes a polling-based own-order event stream abstraction (`status` + `fill` events), so runner can consume incremental execution updates before the next full runtime reconcile.
- When `websocket-client` is installed, live mode will also open Polymarket's authenticated user channel (`USER_STREAM_URL`) and buffer `order` / `trade` events in the broker; if the stream dependency is missing or the connection drops, the bot automatically falls back to the existing polling-based reconcile path.
- Runtime snapshots now persist the last broker-event watermark, so restart recovery can resume incremental reconcile without widening the replay window back to zero every time.
- Trader startup now records a readiness checklist into the event log, ledger, and daemon state. In live mode that checklist includes broker capability checks plus the latest `NETWORK_SMOKE_LOG` result, and will mark the process not-ready if the last smoke run reported a block or endpoint failure, the smoke record is stale, or the explicit live admission flags are not set.
- Daemon state now also exposes an execution reconciliation summary, including `internal_vs_ledger_diff`, pending-order staleness, snapshot age, broker reconcile age, and broker floor gap, so monitoring can distinguish “strategy looked quiet” from “execution facts have drifted”.
- Admission gate is fail-closed and is the only opening truth: when `opening_allowed=false`, BUY is rejected even if legacy compatibility fields still look normal.
- `/api/state` now exposes `admission` fields for operators: `mode`, `opening_allowed`, `reduce_only`, `halted`, `reason_codes`, and `evidence_summary` (`account_snapshot_age_seconds`, `broker_event_sync_age_seconds`, `ledger_diff`, `reconciliation_status`).
- `HALTED` uses allowlist semantics (sync read / state evaluation / cancel pending BUY / required persistence updates only). `REDUCE_ONLY` blocks any net-exposure increase, including indirect replace/modify/retry paths.
- Admission snapshot persistence is output-only (`mode` + `reason_codes` + evidence summary). Restart always begins protected and re-evaluates using fresh startup/reconciliation/staleness evidence before reopening BUY.
- BLOCK-004 self-checks: run `PYTHONPATH=src .venv/bin/python scripts/verify_fail_closed_startup.py` and `PYTHONPATH=src .venv/bin/python scripts/verify_untrusted_state_blocks_buy.py`; full gate is `bash scripts/gates/gate_block_item.sh BLOCK-004`.
- Kill switch is now a broker-terminal state machine (`REQUESTED -> CANCELING_BUY -> WAITING_BROKER_TERMINAL -> SAFE_CONFIRMED / FAILED_MANUAL_REQUIRED`) rather than a local boolean toggle.
- `pause_opening` immediately blocks new BUY; `reduce_only` additionally requires pending/open BUY cleanup to broker terminal; `emergency_stop` enters strict `halted+latched` protection until broker safety is confirmed.
- `cancel_requested` / queued cancel is not treated as safe; local pending cleanup alone is insufficient without broker terminal evidence.
- Kill switch inflight state is persisted in runtime truth and recovered on restart, so “cancel requested but unconfirmed” keeps blocking BUY after process restart.
- `/api/state` now exposes `kill_switch` operator fields: `phase`, `mode_requested`, `broker_safe_confirmed`, `open_buy_order_ids`, `non_terminal_buy_order_ids`, `reason_codes`, and `manual_required`.
- BLOCK-005 self-checks: run `PYTHONPATH=src .venv/bin/python scripts/verify_kill_switch_terminal.py` and `PYTHONPATH=src .venv/bin/python scripts/verify_reduce_only_terminal_cleanup.py`; full gate is `bash scripts/gates/gate_block_item.sh BLOCK-005`.
- Control-plane write access is now fail-closed behind a single `write_api_available` verdict shared by startup checks, POST routing, and `/api/state` export (`control_plane_security`).
- Any write route requires both a valid control token and an allowed source address; no empty-token compatibility bypass remains in write mode.
- Source policy defaults to `local_only`; `X-Forwarded-For` is ignored unless `POLY_TRUSTED_PROXY_CIDRS` is explicitly configured and the socket peer is in that trusted proxy list.
- Live mode (`DRY_RUN=false`) blocks startup when `POLY_ENABLE_WRITE_API=true` but `POLY_CONTROL_TOKEN` is missing or weak (length < `CONTROL_TOKEN_MIN_LENGTH` or known weak value).
- Write requests now emit audit records for both rejected and successful flows at `CONTROL_AUDIT_LOG_PATH` (default runtime namespace `control_audit_events.jsonl`).
- `/api/state` exposes minimal control-plane security fields only: `token_configured`, `write_api_available`, `write_api_enabled`, `readonly_mode`, `live_mode`, `source_policy`, `trusted_proxy_configured`, `reason_codes`.
- BLOCK-006 self-checks: run `PYTHONPATH=src .venv/bin/python scripts/verify_control_auth.py` and `PYTHONPATH=src .venv/bin/python scripts/verify_write_api_local_only.py`; full gate is `bash scripts/gates/gate_block_item.sh BLOCK-006`.
- Live signer boundary is now fail-closed: `PRIVATE_KEY` is forbidden in live mode, and startup requires `SIGNER_URL` + `CLOB_API_*` credentials + identity binding (`signer/api/broker == FUNDER_ADDRESS`).
- Live submit path only accepts minimal signer output (`signed_order`); signer responses that contain secret-like materials are rejected.
- Startup and runtime enforce `LIVE_HOT_WALLET_BALANCE_CAP_USD`: startup exceedance fails readiness; runtime exceedance latches recovery conflict and blocks BUY.
- `/api/state` includes minimal `signer_security` summary fields (`signer_healthy`, identity match booleans, cap status, reason codes) without exposing raw key/token/topology internals.
- BLOCK-007 self-checks: run `PYTHONPATH=src:tests .venv/bin/python scripts/verify_signer_required_live.py` and `PYTHONPATH=src:tests .venv/bin/python scripts/verify_no_raw_key_in_live_mode.py`; full gate is `bash scripts/gates/gate_block_item.sh BLOCK-007`.
- External observability now has a fixed, external metrics projection (`/metrics`) derived from the same request snapshot used by `/api/state` (`observability`), avoiding per-request dual refresh drift.
- `runner_heartbeat` is updated only by the active runner loop; read-only web/standby paths do not write heartbeat and cannot fake healthy loop freshness.
- `buy_blocked_duration_seconds` semantics are fixed: start on first BUY block transition (`opening_allowed -> false`), clear on unblock (`opening_allowed -> true`), and export both state and metrics consistently.
- Alert routing uses a fixed `alert_code` whitelist with fixed `page|warning` mapping; metrics labels are low-cardinality only (`alert_code`, `severity`), never raw `reason_codes`, wallet, order id, or exception text.
- `/metrics` is side-effect free: no heartbeat update, no runtime refresh, no recovery probe, no cache/public-state write.
- BLOCK-008 self-checks: run `PYTHONPATH=src .venv/bin/python scripts/verify_metrics_and_alerts.py` and `PYTHONPATH=src .venv/bin/python scripts/verify_heartbeat_staleness_alert.py`; full gate is `bash scripts/gates/gate_block_item.sh BLOCK-008`.
- Exposure / breaker docs for BLOCK-009 live in `docs/runbook/exposure_and_breakers.md`.
- BLOCK-009 keeps all exposure math in USD terms so ledger, cap, and breaker checks use one unit of account.
- The three cap layers are evaluated together on every BUY path: exposure ledger limit, wallet/portfolio cap, and breaker state; condition cap joins the same decision when `PORTFOLIO_NETTING_ENABLED=true`.
- When multiple risk reasons are present, the system reports the primary reason first and keeps secondary reasons as supporting evidence.
- Loss streak counting increments on realized losing closes, resets on a winning close, and clears on full state reset or explicit recovery reset.
- Intraday drawdown is measured on the configured local trading day boundary (`RISK_BREAKER_TIMEZONE`), and latches only when threshold breach is paired with negative realized-loss evidence.
- Breakers clear only after their release condition is met and the protected state has been re-evaluated as healthy; otherwise they stay latched.
- Any risk fault is fail-closed: if breaker state cannot be trusted, BUY remains blocked until the risk state is rebuilt and exported cleanly.
- BLOCK-009 self-checks: run `PYTHONPATH=src .venv/bin/python scripts/verify_exposure_caps.py` and `PYTHONPATH=src .venv/bin/python scripts/verify_loss_streak_and_drawdown_breakers.py`; full gate is `bash scripts/gates/gate_block_item.sh BLOCK-009`.

## Environment Notes

- Keep real secrets in `.env` and never commit them.
- `.env.example` is the safe template; update it when you add new settings.
- For paper trading, you can leave `PRIVATE_KEY` and `FUNDER_ADDRESS` empty.
- For live trading, use `FUNDER_ADDRESS` + `SIGNER_URL` + `CLOB_API_*`; `PRIVATE_KEY` must stay empty (forbidden in live mode).
- Concurrency guardrails: `ENABLE_SINGLE_WRITER` (default true) acquires `WALLET_LOCK_PATH`; `STATE_STORE_PATH` hosts runtime/control/idempotency data; `IDEMPOTENCY_WINDOW_SECONDS` controls duplicate suppression window.
- Control-plane security knobs: `POLY_ENABLE_WRITE_API`, `POLY_CONTROL_TOKEN`, `CONTROL_TOKEN_MIN_LENGTH`, `POLY_CONTROL_SOURCE_POLICY` (`local_only|internal_only|any`), `POLY_TRUSTED_PROXY_CIDRS`, `POLY_CONTROL_AUDIT_LOG_PATH`.
- Runtime recovery and conflict handling details: `docs/runbook/runtime_state_recovery.md`.
- Single-writer lifecycle, scope rule, and standby behavior: `docs/runbook/single_writer_lock.md`.
- Kill switch broker-terminal confirmation and restart recovery runbook: `docs/runbook/kill_switch_terminal_confirmation.md`.
- Control-plane auth, source policy, and write/read boundary runbook: `docs/runbook/control_plane_security.md`.
- Signer/secret boundary and hot-wallet-cap runbook: `docs/runbook/signer_and_secret_boundary.md`.
- Observability/heartbeat/metrics/alert mapping runbook: `docs/runbook/observability_and_alerting.md`.
- Exposure / loss-streak / drawdown breaker runbook: `docs/runbook/exposure_and_breakers.md`.
- `BLOCKBEATS_API_KEY` is optional for the trading engine, but required for the shared BlockBeats skill. The repo helper `scripts/blockbeats_query.sh` will also use it and can load it from `.env`.

Before switching to live-like testing, run:

```bash
make network-smoke
```

You can also override log destination:

```bash
NETWORK_SMOKE_LOG=/tmp/poly_network_smoke.jsonl make network-smoke
```

## Notes

- This is still a framework you can extend further (stop-loss, TP, deeper netting, market filters).
- `LiveClobBroker` expects `py-clob-client` API compatibility; if the upstream SDK changes, adjust the order call in `src/polymarket_bot/brokers/live_clob.py`.
- No guarantee of profitability. Use strict risk limits.

## Monitoring Reports

- 30m 报告（持续观察，不触发交易参数修改）:
  - `make monitor-30m`
  - 默认产物: `/tmp/poly_monitor_30m_report.txt`
  - JSON 产物: `/tmp/poly_monitor_30m_report.json`
  - 脚本: `scripts/monitor_thresholds_30m.sh`
  - 现在会同时读取 daemon `state.json` 中的 `startup` / `reconciliation` 摘要，报告里会额外标出账本漂移、pending 单陈旧和同步年龄。

- 12h 报告（持续观察，不触发交易参数修改）:
  - `make monitor-12h`
  - 默认产物: `/tmp/poly_monitor_12h_report.txt`
  - JSON 产物: `/tmp/poly_monitor_12h_report.json`
  - 脚本: `scripts/monitor_thresholds_12h.sh`
  - 同样会把 `startup_ready` 和 `reconciliation.status` 纳入最终建议，不再只看日志计数。

- 独立日终对账报告:
  - `make reconciliation-report`
  - 默认产物: `/tmp/poly_reconciliation_eod_report.txt`
  - JSON 产物: `/tmp/poly_reconciliation_eod_report.json`
  - 脚本: `scripts/generate_reconciliation_report.py`
  - 报告会汇总当日 ledger fill/account_sync/startup_checks、state 里的 reconciliation 摘要，以及按 `source` / `side` 的成交分解。

- 全流程验收报告:
  - `make full-validate`
  - 默认产物: `/tmp/poly_full_flow_validation_report.txt`
  - JSON 产物: `/tmp/poly_full_flow_validation_report.json`
  - 脚本: `scripts/full_flow_validate.py`
  - 默认会先重启本地 stack，再用 `0s` quick window 重建 monitor 报告、刷新 EOD 对账、验证 `/api/state` + `/api/monitor/*` + `/api/reconciliation/eod`，最后跑一遍 runtime replay / replay-calibration。
  - 报告会把“流程是否打通”与“当前 readiness 是 READY / OBSERVE / ESCALATE / BLOCK”分开呈现，方便先验收链路，再看运营门禁。

- 同时运行两档报告（长期后台）:
  - 直接前台运行: `make monitor-reports`（默认 both）
  - 后台 stop/clear:
    - `make stop-monitor-reports`
    - `make monitor-scheduler-install` 写入 `LaunchAgent`（一次安装后可自动常驻）
      - 安装脚本会自动尝试 `launchd`，若无权限则自动降级为 `nohup` 后台常驻；
      - 如在 Desktop/受限目录里想直接绕过 `launchd`，可用 `MONITOR_FORCE_NOHUP=1 make monitor-scheduler-install`
      - 如想完全手工可直接用 `make monitor-reports`.
    - `make monitor-scheduler-uninstall`
    - `make monitor-scheduler-status`

支持参数:
  - `MONITOR_MODE=30m|12h|both`
  - `ROTATE_KEEP=<保留日志文件数，默认 24>`
  - `MONITOR_DAEMON_LOG=<daemon 日志路径，默认 /tmp/poly_runtime_data/poly_bot.log>`
  - API 可直接读取:
    - `/api/monitor/30m`
    - `/api/monitor/12h`
    - `/api/reconciliation/eod`
    - `POST /api/operator` with `{"command":"generate_reconciliation_report"}` 可直接刷新 EOD 对账产物
    - `POST /api/operator` with `{"command":"clear_stale_pending"}` 会请求 runner 在下一轮清理已超时的 pending 单
  - Dashboard 现在会直接消费这 3 个 JSON 接口，显示 monitor 最终建议、EOD 对账状态和成交分解，不再只展示 `/api/state`。
  - 顶部还会有一层 operator gate banner，把 `READY / OBSERVE / ESCALATE / BLOCK` 结论直接顶到最上面，并在异常时高亮紧急退出按钮。
  - Gate banner 还会自动生成建议动作清单，例如 `make network-smoke`、处理 stale pending、核对 ledger drift，减少 live 排障时的来回切换。
  - 建议动作支持直接交互：可以复制排障命令、跳到订单/监控面板，或直接打开 `/api/monitor/*`、`/api/reconciliation/eod` JSON。
  - 其中 EOD 对账现在也支持从 dashboard 直接触发刷新，不必手工回终端执行。
  - Dashboard 还新增了“执行诊断明细”面板，直接展开 startup checks、reconciliation facts 和 monitor/EOD 重点问题，作为 operator gate 的下钻视图。
  - 诊断面板现在还会展开活跃 pending/stale 订单，并支持点击 startup check 或 pending order 查看结构化焦点详情。
  - `clear_stale_pending` 的最近一次执行结果也会回写到 runtime state，并在诊断面板里显示 `REQUESTED / CLEARED / NOOP` 状态。

- 10 小时实盘前演练（paper）：
  - `make rehearse-10h`
  - 兼容旧入口: `make rehearse-12h`（已切换为 10h）
  - 每小时自动写 1 条 checkpoint
  - 结果落盘: `/tmp/poly_10h_paper_rehearsal.txt`
  - 运行日志: `/tmp/poly_10h_paper_rehearsal.log`
  - 查看最新进度: `make rehearse-progress`

- 轻量回放 / 参数校准：
  - `make replay`
  - `make replay-calibrate`
  - 直接 JSON 输出: `.venv/bin/python scripts/replay_calibration.py --json`
  - 按题材切片: `.venv/bin/python scripts/replay_calibration.py --topic 加密`
  - 列出钱包池版本: `.venv/bin/python scripts/replay_calibration.py --list-wallet-pools`
  - 按钱包池版本切片: `.venv/bin/python scripts/replay_calibration.py --wallet-pool 1a2b3c4d`
  - 自定义场景文件: `.venv/bin/python scripts/replay_calibration.py --scenario-file /path/to/scenarios.json`
  - 现在会同时输出 `gross_cashflow` 和 `net_cashflow`，并支持用 `REPLAY_TAKER_FEE_BPS`、`REPLAY_ENTRY_SLIPPAGE_BPS`、`REPLAY_EXIT_SLIPPAGE_BPS`、`REPLAY_FEE_KEYWORDS` 对 fee-enabled 市场做费用/滑点敏感性校准
  - 还支持 spread-aware 滑点参数：`REPLAY_ENTRY_SPREAD_MULTIPLIER`、`REPLAY_EXIT_SPREAD_MULTIPLIER`、`REPLAY_EDGE_PRICE_PENALTY_BPS`
  - replay 现在也会纳入 live reconcile 产生的 `order_reconciled` / `order_partial_fill` 样本

## BLOCK-010 Final Release Gate (GO/NO-GO)

- 最终上线门禁统一入口：`bash scripts/gates/gate_release_readiness.sh`
- required blocks 单一来源：`scripts/gates/release_blocks.json`（当前为 `BLOCK-001` 到 `BLOCK-009`）
- 判定规则（默认 fail-closed）：
  - 任一 required block 失败 => `NO-GO`
  - 任一 required block 缺失 machine result 或报告结构不合格 => `NO-GO`
  - 仅当全部 required blocks 通过时才允许 `GO`
- release 报告（原子落盘）：
  - 机器可读：`reports/release/go_no_go_summary.json`
  - 人类可读：`reports/release/go_no_go_summary.md`
- 报告包含固定元数据：
  - `git commit`
  - `git branch`
  - `execution timestamp`
  - `release gate command`
  - `required blocks` 列表
- 运行与故障处理说明见：
  - `docs/runbook/release_gating_and_go_no_go.md`
  - `docs/blocking/final_release_checklist.md`
