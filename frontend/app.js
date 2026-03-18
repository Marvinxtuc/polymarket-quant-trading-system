(function () {
  const $ = (id) => document.getElementById(id);
  const FRONTEND_REFRESH_SECONDS = 5;
  const controlState = {
    pause_opening: false,
    reduce_only: false,
    emergency_stop: false,
    updated_ts: 0,
  };
  const STORAGE_KEYS = {
    exitReviewFilter: "poly.exitReviewFilter",
    selectedExitSampleKey: "poly.selectedExitSampleKey",
    selectedTraceId: "poly.selectedTraceId",
    selectedSignalCycleId: "poly.selectedSignalCycleId",
    selectedAttributionWindow: "poly.selectedAttributionWindow",
  };
  let selectedWallet = "";
  let selectedTraceId = "";
  let selectedSignalCycleId = "";
  let selectedAttributionWindow = "24h";
  let selectedDiagnosticFocusKind = "";
  let selectedDiagnosticFocusKey = "";
  const exitReviewFilter = {
    kind: "",
    topic: "",
    source: "",
  };
  let selectedExitSampleKey = "";
  let lastExitReview = null;
  let lastExitReviewNow = 0;
  let lastSignalReview = null;
  let lastSignalReviewNow = 0;
  let lastAttributionReview = null;
  let lastAttributionReviewNow = 0;
  let lastDiagnosticsState = null;
  let lastDiagnosticsMonitor30 = null;
  let lastDiagnosticsMonitor12 = null;
  let lastDiagnosticsEod = null;
  let lastDiagnosticsNow = 0;
  const EMPTY_MONITOR_REPORT = (reportType) => ({
    report_type: reportType,
    generated_ts: 0,
    window_start: "",
    window_end: "",
    window_seconds: 0,
    log_file: "",
    sample_status: "unknown",
    counts: {},
    ratios: {},
    recommendation: "",
    final_recommendation: "",
    consecutive_inconclusive_windows: 0,
    daemon_state_file: "",
    startup_ready: null,
    startup: {},
    reconciliation_status: "unknown",
    reconciliation_issue_summary: "",
    reconciliation: {},
  });
  const EMPTY_RECONCILIATION_EOD_REPORT = {
    report_version: 1,
    generated_ts: 0,
    generated_at: "",
    day_key: "",
    state_path: "",
    ledger_path: "",
    status: "unknown",
    issues: [],
    startup: {},
    reconciliation: {},
    state_summary: {},
    ledger_summary: {},
    recommendations: [],
  };

  function loadUiState() {
    try {
      const rawFilter = window.localStorage.getItem(STORAGE_KEYS.exitReviewFilter);
      if (rawFilter) {
        const parsed = JSON.parse(rawFilter);
        if (parsed && typeof parsed === "object") {
          exitReviewFilter.kind = String(parsed.kind || "");
          exitReviewFilter.topic = String(parsed.topic || "");
          exitReviewFilter.source = String(parsed.source || "");
        }
      }
      selectedExitSampleKey = String(window.localStorage.getItem(STORAGE_KEYS.selectedExitSampleKey) || "");
      selectedTraceId = String(window.localStorage.getItem(STORAGE_KEYS.selectedTraceId) || "");
      selectedSignalCycleId = String(window.localStorage.getItem(STORAGE_KEYS.selectedSignalCycleId) || "");
      selectedAttributionWindow = String(window.localStorage.getItem(STORAGE_KEYS.selectedAttributionWindow) || "24h");
    } catch (_err) {
      // ignore storage failures
    }
  }

  function persistExitReviewUiState() {
    try {
      window.localStorage.setItem(
        STORAGE_KEYS.exitReviewFilter,
        JSON.stringify({
          kind: String(exitReviewFilter.kind || ""),
          topic: String(exitReviewFilter.topic || ""),
          source: String(exitReviewFilter.source || ""),
        })
      );
      window.localStorage.setItem(STORAGE_KEYS.selectedExitSampleKey, String(selectedExitSampleKey || ""));
      window.localStorage.setItem(STORAGE_KEYS.selectedTraceId, String(selectedTraceId || ""));
      window.localStorage.setItem(STORAGE_KEYS.selectedSignalCycleId, String(selectedSignalCycleId || ""));
      window.localStorage.setItem(STORAGE_KEYS.selectedAttributionWindow, String(selectedAttributionWindow || "24h"));
    } catch (_err) {
      // ignore storage failures
    }
  }

  function fmtUsd(n, showSign = true) {
    const v = Number(n || 0);
    const sign = showSign && v > 0 ? "+" : "";
    return `${sign}$${v.toFixed(2)}`;
  }

  function fmtPct(n, digits = 2) {
    return `${Number(n || 0).toFixed(digits)}%`;
  }

  function fmtRatioPct(n, digits = 1) {
    return fmtPct(Number(n || 0) * 100, digits);
  }

  function fmtSignedRatioPct(n, digits = 1) {
    const v = Number(n || 0) * 100;
    const sign = v > 0 ? "+" : "";
    return `${sign}${v.toFixed(digits)}%`;
  }

  function hhmm(ts) {
    const d = new Date((ts || 0) * 1000);
    const hh = String(d.getHours()).padStart(2, "0");
    const mm = String(d.getMinutes()).padStart(2, "0");
    const ss = String(d.getSeconds()).padStart(2, "0");
    return `${hh}:${mm}:${ss}`;
  }

  function fmtAge(seconds) {
    const s = Math.max(0, Number(seconds || 0));
    if (s < 60) return `${s}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m`;
    return `${Math.floor(s / 3600)}h`;
  }

  function fmtDateTime(ts) {
    const value = Number(ts || 0);
    if (value <= 0) return "--";
    const d = new Date(value * 1000);
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    const hh = String(d.getHours()).padStart(2, "0");
    const min = String(d.getMinutes()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd} ${hh}:${min}`;
  }

  function fmtHoldMinutes(minutes) {
    const mins = Math.max(0, Number(minutes || 0));
    if (mins < 60) return `${mins}m`;
    const hours = Math.floor(mins / 60);
    const rem = mins % 60;
    if (hours < 24) return rem > 0 ? `${hours}h ${rem}m` : `${hours}h`;
    const days = Math.floor(hours / 24);
    const hourRem = hours % 24;
    return hourRem > 0 ? `${days}d ${hourRem}h` : `${days}d`;
  }

  function replaceRows(tbody, rows, fallbackRow) {
    if (!tbody) return;
    tbody.innerHTML = rows.length > 0 ? rows.join("") : fallbackRow;
  }

  function clsForWeight(w) {
    if (w >= 0.8) return "w-high";
    if (w >= 0.6) return "w-mid";
    return "w-low";
  }

  function clsForScore(score) {
    const v = Number(score || 0);
    if (v >= 80) return "w-high";
    if (v >= 65) return "w-mid";
    return "w-low";
  }

  function clsForValue(value) {
    const v = Number(value || 0);
    if (v > 0) return "value-positive";
    if (v < 0) return "value-negative";
    return "value-neutral";
  }

  function tierTagClass(tier) {
    const v = String(tier || "").toUpperCase();
    if (v === "CORE") return "ok";
    if (v === "TRADE") return "wait";
    if (v === "WATCH") return "cancel";
    return "danger";
  }

  function controlLabel() {
    if (controlState.emergency_stop) return "紧急退出";
    if (controlState.reduce_only) return "只减仓";
    if (controlState.pause_opening) return "暂停开仓";
    return "正常";
  }

  function shortWallet(value) {
    const wallet = String(value || "");
    if (wallet.length <= 14) return wallet || "-";
    return `${wallet.slice(0, 8)}...${wallet.slice(-4)}`;
  }

  function normalizeWallet(value) {
    return String(value || "").trim().toLowerCase();
  }

  function historyProfile(row, config) {
    const closed = Number(row.closed_positions || 0);
    const resolved = Number(row.resolved_markets || 0);
    const minClosed = Math.max(1, Number(config.history_min_closed_positions || 5));
    const strongClosed = Math.max(minClosed, Number(config.history_strong_closed_positions || 15));
    const strongResolved = Math.max(1, Number(config.history_strong_resolved_markets || 10));

    if (closed <= 0) {
      return {
        cls: "danger",
        label: "缺失",
        detail: "暂无结算历史",
      };
    }
    if (closed < minClosed) {
      return {
        cls: "cancel",
        label: "偏薄",
        detail: `closed ${closed} / min ${minClosed}`,
      };
    }
    if (closed >= strongClosed || resolved >= strongResolved) {
      return {
        cls: "ok",
        label: "充分",
        detail: `closed ${closed} / resolved ${resolved}`,
      };
    }
    return {
      cls: "wait",
      label: "可用",
      detail: `closed ${closed} / resolved ${resolved}`,
    };
  }

  function historyAgeLabel(ts, now) {
    const refreshTs = Number(ts || 0);
    if (refreshTs <= 0 || now <= 0) return "未刷新";
    return `${fmtAge(Math.max(0, now - refreshTs))}前`;
  }

  function componentLabel(key) {
    const labels = {
      notional: "仓位规模",
      positions: "活跃仓位",
      unique_markets: "市场分散",
      concentration: "集中度",
      activity: "近期活跃",
      history_win_rate: "历史胜率",
      history_roi: "历史 ROI",
      history_profit_factor: "盈亏比",
      history_resolution: "解析命中",
    };
    return labels[key] || key;
  }

  function sampleVerdict(sample) {
    if (sample && sample.resolved === true) {
      if (sample.resolved_correct === true) return ["ok", "命中"];
      if (sample.resolved_correct === false) return ["danger", "失手"];
    }
    const pnl = Number(sample && sample.realized_pnl || 0);
    if (pnl > 0) return ["ok", "盈利"];
    if (pnl < 0) return ["cancel", "亏损"];
    return ["wait", "持平"];
  }

  function topicTone(topic) {
    const roi = Number(topic && topic.roi || 0);
    const winRate = Number(topic && topic.win_rate || 0);
    if (roi > 0.12 || winRate >= 0.7) return "value-positive";
    if (roi < 0 || winRate < 0.45) return "value-negative";
    return "warn";
  }

  function exitTagMeta(kind, fallbackLabel = "") {
    const value = String(kind || "").trim().toLowerCase();
    if (value === "resonance_exit") return ["danger", fallbackLabel || "共振退出"];
    if (value === "smart_wallet_exit") return ["wait", fallbackLabel || "主钱包减仓"];
    if (value === "time_exit") return ["cancel", fallbackLabel || "时间退出"];
    if (value === "emergency_exit") return ["danger", fallbackLabel || "紧急退出"];
    return ["ok", fallbackLabel || "开仓"];
  }

  function exitResultMeta(result, fallbackLabel = "") {
    const value = String(result || "").trim().toLowerCase();
    if (value === "emergency") return ["danger", fallbackLabel || "紧急退出"];
    if (value === "full_exit") return ["ok", fallbackLabel || "完全退出"];
    if (value === "partial_trim") return ["wait", fallbackLabel || "部分减仓"];
    if (value === "reject") return ["danger", fallbackLabel || "已拒绝"];
    return ["cancel", fallbackLabel || "未标记"];
  }

  function reportStatusMeta(status) {
    const value = String(status || "").trim().toLowerCase();
    if (value === "ok" || value === "ready") return ["ok", "OK"];
    if (value === "warn" || value === "warning") return ["wait", "WARN"];
    if (value === "fail" || value === "blocked" || value === "error") return ["danger", "FAIL"];
    if (value === "conclusive") return ["ok", "CONCLUSIVE"];
    if (value === "inconclusive") return ["cancel", "INCONCLUSIVE"];
    return ["cancel", String(status || "UNKNOWN").toUpperCase() || "UNKNOWN"];
  }

  function startupCheckMeta(status) {
    const value = String(status || "").trim().toUpperCase();
    if (value === "PASS") return ["ok", "PASS"];
    if (value === "WARN") return ["wait", "WARN"];
    if (value === "FAIL") return ["danger", "FAIL"];
    return ["cancel", value || "UNKNOWN"];
  }

  function reportDecisionMeta(text, fallbackStatus = "") {
    const value = String(text || "").trim();
    const upper = value.toUpperCase();
    if (upper.startsWith("BLOCK")) return ["danger", "BLOCK"];
    if (upper.startsWith("ESCALATE")) return ["danger", "ESCALATE"];
    if (upper.startsWith("OBSERVE")) return ["wait", "OBSERVE"];
    if (upper.startsWith("NO ESCALATION")) return ["ok", "OK"];
    if (upper.startsWith("CONSECUTIVE_INCONCLUSIVE")) return ["cancel", "INCONCLUSIVE"];
    return reportStatusMeta(fallbackStatus || "unknown");
  }

  function recommendationKind(text) {
    const upper = String(text || "").trim().toUpperCase();
    if (upper.startsWith("BLOCK")) return "block";
    if (upper.startsWith("ESCALATE")) return "escalate";
    if (upper.startsWith("OBSERVE")) return "observe";
    if (upper.startsWith("CONSECUTIVE_INCONCLUSIVE")) return "observe";
    return "ready";
  }

  async function fetchJson(path, fallback) {
    try {
      const res = await fetch(path, { cache: "no-store" });
      if (!res.ok) return fallback;
      const data = await res.json();
      if (!data || typeof data !== "object") return fallback;
      return data;
    } catch (_err) {
      return fallback;
    }
  }

  async function copyText(text) {
    const value = String(text || "");
    if (!value) return false;
    try {
      if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
        await navigator.clipboard.writeText(value);
        return true;
      }
    } catch (_err) {
      // fall through
    }
    try {
      const textarea = document.createElement("textarea");
      textarea.value = value;
      textarea.setAttribute("readonly", "true");
      textarea.style.position = "fixed";
      textarea.style.opacity = "0";
      document.body.appendChild(textarea);
      textarea.focus();
      textarea.select();
      const ok = document.execCommand("copy");
      document.body.removeChild(textarea);
      return !!ok;
    } catch (_err) {
      return false;
    }
  }

  function computeOpsGate(data, monitor30m, monitor12h, eodReport) {
    const startup = data && data.startup && typeof data.startup === "object" ? data.startup : {};
    const reconciliation = data && data.reconciliation && typeof data.reconciliation === "object" ? data.reconciliation : {};
    const eod = eodReport && typeof eodReport === "object" ? eodReport : EMPTY_RECONCILIATION_EOD_REPORT;
    const report30 = monitor30m && typeof monitor30m === "object" ? monitor30m : EMPTY_MONITOR_REPORT("monitor_30m");
    const report12 = monitor12h && typeof monitor12h === "object" ? monitor12h : EMPTY_MONITOR_REPORT("monitor_12h");
    const checks = Array.isArray(startup.checks) ? startup.checks : [];
    const eodIssues = Array.isArray(eod.issues) ? eod.issues : [];
    const actions = [];
    const actionKeys = new Set();

    const pushAction = (cls, title, detail, controls = []) => {
      const safeTitle = String(title || "").trim();
      const safeDetail = String(detail || "").trim();
      if (!safeTitle || !safeDetail) return;
      const key = `${safeTitle}::${safeDetail}`;
      if (actionKeys.has(key)) return;
      actionKeys.add(key);
      const normalizedControls = Array.isArray(controls)
        ? controls
            .filter((item) => item && typeof item === "object")
            .map((item) => ({
              type: String(item.type || "").trim(),
              label: String(item.label || "").trim(),
              value: String(item.value || "").trim(),
            }))
            .filter((item) => item.type && item.label && item.value)
        : [];
      actions.push({ cls, title: safeTitle, detail: safeDetail, controls: normalizedControls });
    };

    let level = "ready";
    if (startup.ready === false || recommendationKind(report30.final_recommendation) === "block" || recommendationKind(report12.final_recommendation) === "block") {
      level = "block";
    } else if (String(reconciliation.status || "").toLowerCase() === "fail" || String(eod.status || "").toLowerCase() === "fail" || recommendationKind(report30.final_recommendation) === "escalate" || recommendationKind(report12.final_recommendation) === "escalate") {
      level = "escalate";
    } else if (String(reconciliation.status || "").toLowerCase() === "warn" || String(eod.status || "").toLowerCase() === "warn" || recommendationKind(report30.final_recommendation) === "observe" || recommendationKind(report12.final_recommendation) === "observe") {
      level = "observe";
    }

    let title = "执行门禁已就绪";
    let detail = "startup、自检、monitor 和 reconciliation 当前没有阻断项，可以继续观察策略与执行质量。";
    if (level === "block") {
      title = "运行门禁阻断";
      detail = String(report12.final_recommendation || report30.final_recommendation || "启动自检未通过，或 live 前置条件缺失。优先修复环境与账户问题。");
    } else if (level === "escalate") {
      title = "执行对账需要升级处理";
      detail = String(report12.final_recommendation || report30.final_recommendation || "账本与 broker 事实层可能已经漂移，建议先停参数变更。");
    } else if (level === "observe") {
      title = "运行中有警告，先观察再调参";
      detail = String(report12.final_recommendation || report30.final_recommendation || "当前更多像执行告警而不是策略信号结论。");
    }

    const items = [
      {
        label: "启动自检",
        value: startup.ready === false ? `${Number(startup.failure_count || 0)} fail / ${Number(startup.warning_count || 0)} warn` : startup.ready === true ? "ready" : "unknown",
      },
      {
        label: "30m",
        value: String(report30.final_recommendation || report30.recommendation || report30.sample_status || "unknown"),
      },
      {
        label: "12h",
        value: String(report12.final_recommendation || report12.recommendation || report12.sample_status || "unknown"),
      },
      {
        label: "对账",
        value: String(reconciliation.status || eod.status || "unknown"),
      },
    ];

    const issueParts = [];
    for (const row of checks.slice(0, 4)) {
      const status = String(row && row.status || "");
      if (status === "FAIL" || status === "WARN") {
        issueParts.push(`${String(row.name || "startup")}: ${String(row.message || status)}`);
      }
    }
    for (const item of eodIssues.slice(0, 4)) {
      issueParts.push(String(item));
    }

    for (const row of checks) {
      const name = String(row && row.name || "").trim();
      const status = String(row && row.status || "").trim().toUpperCase();
      const message = String(row && row.message || "").trim();
      if (status !== "FAIL" && status !== "WARN") continue;
      if (name === "network_smoke") {
        pushAction(
          status === "FAIL" ? "danger" : "wait",
          "重跑 network smoke",
          `先执行 <code>make network-smoke</code>，确认 geoblock / endpoint 正常；当前提示: ${message || "network smoke 需要复核"}.`,
          [
            { type: "copy", label: "复制命令", value: "make network-smoke" },
            { type: "open", label: "打开状态 JSON", value: "/api/state" },
            { type: "jump", label: "跳到诊断", value: "diagnostics-panel" },
          ]
        );
      } else if (name === "api_credentials" || name === "funder_address" || name === "signature_type") {
        pushAction(
          "danger",
          "核对 live 账户与签名配置",
          `检查 <code>PRIVATE_KEY</code>、<code>FUNDER_ADDRESS</code>、<code>CLOB_SIGNATURE_TYPE</code>，确保 API credentials 能稳定派生；当前提示: ${message || name}.`,
          [
            { type: "open", label: "打开状态 JSON", value: "/api/state" },
            { type: "jump", label: "跳到诊断", value: "diagnostics-panel" },
          ]
        );
      } else if (name === "market_preflight") {
        pushAction(
          "danger",
          "恢复 market preflight",
          `确认 live broker 已配置 market client，否则 book / midpoint / tick preflight 会直接拒单；当前提示: ${message || name}.`,
          [
            { type: "open", label: "打开状态 JSON", value: "/api/state" },
            { type: "jump", label: "跳到诊断", value: "diagnostics-panel" },
          ]
        );
      } else if (name === "order_status_support") {
        pushAction(
          "danger",
          "修复 broker 订单查询能力",
          `先确认 <code>py-clob-client</code> 版本和适配层，保证 <code>get_order</code> / <code>get_orders</code> 可用；当前提示: ${message || name}.`,
          [
            { type: "open", label: "打开状态 JSON", value: "/api/state" },
            { type: "jump", label: "跳到诊断", value: "diagnostics-panel" },
          ]
        );
      } else if (name === "heartbeat_support") {
        pushAction(
          "wait",
          "确认 heartbeat 策略",
          `当前 SDK 可能缺少 heartbeat 能力，先避免依赖长时间 resting orders；当前提示: ${message || name}.`,
          [
            { type: "open", label: "打开状态 JSON", value: "/api/state" },
            { type: "jump", label: "跳到诊断", value: "diagnostics-panel" },
          ]
        );
      } else if (name === "user_stream") {
        pushAction(
          "wait",
          "检查 user stream 依赖",
          `安装 <code>websocket-client</code> 并确认 <code>USER_STREAM_*</code> 配置；否则只会退回 polling reconcile；当前提示: ${message || name}.`,
          [
            { type: "copy", label: "复制安装命令", value: ".venv/bin/pip install websocket-client" },
            { type: "open", label: "打开状态 JSON", value: "/api/state" },
            { type: "jump", label: "跳到诊断", value: "diagnostics-panel" },
          ]
        );
      } else if (name === "clob_host") {
        pushAction(
          "danger",
          "确认 CLOB host",
          `检查 live CLOB host 配置与网络连通性；当前提示: ${message || name}.`,
          [
            { type: "open", label: "打开状态 JSON", value: "/api/state" },
            { type: "jump", label: "跳到诊断", value: "diagnostics-panel" },
          ]
        );
      }
    }

    const internalDiff = Math.abs(Number(reconciliation.internal_vs_ledger_diff || (eod.reconciliation && eod.reconciliation.internal_vs_ledger_diff) || 0));
    const stalePending = Number(reconciliation.stale_pending_orders || (eod.reconciliation && eod.reconciliation.stale_pending_orders) || 0);
    const accountAge = Number(reconciliation.account_snapshot_age_seconds || (eod.reconciliation && eod.reconciliation.account_snapshot_age_seconds) || 0);
    const reconcileAge = Number(reconciliation.broker_reconcile_age_seconds || (eod.reconciliation && eod.reconciliation.broker_reconcile_age_seconds) || 0);
    const eventAge = Number(reconciliation.broker_event_sync_age_seconds || (eod.reconciliation && eod.reconciliation.broker_event_sync_age_seconds) || 0);

    if (internalDiff > 0.01) {
      pushAction(
        level === "block" || level === "escalate" ? "danger" : "wait",
        "先核对 ledger 漂移",
        `当前 internal vs ledger 差异为 <code>${fmtUsd(internalDiff)}</code>，先生成 <code>make reconciliation-report</code> 并核对当日 realized PnL。`,
        [
          { type: "api", label: "刷新 EOD 报告", value: "generate_reconciliation_report" },
          { type: "copy", label: "复制命令", value: "make reconciliation-report" },
          { type: "open", label: "打开 EOD JSON", value: "/api/reconciliation/eod" },
          { type: "jump", label: "跳到诊断", value: "diagnostics-panel" },
          { type: "jump", label: "跳到监控面板", value: "monitor-report-panel" },
        ]
      );
    }
    if (stalePending > 0) {
      pushAction(
        stalePending >= 2 ? "danger" : "wait",
        "处理陈旧 pending 单",
        `当前仍有 <code>${stalePending}</code> 个 stale pending orders，先核对 broker open orders / recent fills，必要时撤单后再继续。`,
        [
          { type: "api", label: "清理 stale pending", value: "clear_stale_pending" },
          { type: "jump", label: "跳到订单", value: "orders-panel" },
          { type: "jump", label: "跳到诊断", value: "diagnostics-panel" },
          { type: "jump", label: "跳到监控面板", value: "monitor-report-panel" },
        ]
      );
    }
    if (accountAge > 1800 || reconcileAge > 900 || eventAge > 900) {
      pushAction(
        "wait",
        "刷新 broker / account sync",
        `当前同步时效偏老：account <code>${fmtAge(accountAge)}</code>、reconcile <code>${fmtAge(reconcileAge)}</code>、events <code>${fmtAge(eventAge)}</code>，先确认 daemon 与 user stream 仍在刷新。`,
        [
          { type: "jump", label: "跳到诊断", value: "diagnostics-panel" },
          { type: "jump", label: "跳到监控面板", value: "monitor-report-panel" },
          { type: "open", label: "打开状态 JSON", value: "/api/state" },
        ]
      );
    }

    if (String(report30.sample_status || "").toUpperCase() === "INCONCLUSIVE" || String(report12.sample_status || "").toUpperCase() === "INCONCLUSIVE") {
      const maxInconclusive = Math.max(Number(report30.consecutive_inconclusive_windows || 0), Number(report12.consecutive_inconclusive_windows || 0));
      pushAction(
        "wait",
        "补足样本窗口",
        `当前 monitor 仍有 <code>${maxInconclusive}</code> 个连续 INCONCLUSIVE 窗口，先继续观察 EXEC 样本，不要基于 0 样本调参数。`,
        [
          { type: "jump", label: "跳到诊断", value: "diagnostics-panel" },
          { type: "jump", label: "跳到监控面板", value: "monitor-report-panel" },
        ]
      );
    }

    if (recommendationKind(report30.final_recommendation) !== "ready" || recommendationKind(report12.final_recommendation) !== "ready") {
      pushAction(
        level === "block" || level === "escalate" ? "danger" : "wait",
        "先处理 monitor 摘要里的执行问题",
        `优先阅读 30m / 12h monitor 的最终建议，先解决 skip / reject / exit 比例异常，再讨论策略参数是否要调整。`,
        [
          { type: "jump", label: "跳到诊断", value: "diagnostics-panel" },
          { type: "jump", label: "跳到监控面板", value: "monitor-report-panel" },
          { type: "open", label: "打开 30m JSON", value: "/api/monitor/30m" },
          { type: "open", label: "打开 12h JSON", value: "/api/monitor/12h" },
        ]
      );
    }

    if (actions.length === 0) {
      pushAction(
        "ok",
        "继续观察 shadow / live 一致性",
        `当前门禁通过，继续对比 12h monitor 与 EOD reconciliation 是否保持一致，并关注下一次 startup / broker sync 是否仍然稳定。`,
        [
          { type: "api", label: "刷新 EOD 报告", value: "generate_reconciliation_report" },
          { type: "jump", label: "跳到诊断", value: "diagnostics-panel" },
          { type: "jump", label: "跳到监控面板", value: "monitor-report-panel" },
          { type: "open", label: "打开 EOD JSON", value: "/api/reconciliation/eod" },
        ]
      );
    }

    return {
      level,
      title,
      detail,
      items,
      issues: issueParts,
      actions,
    };
  }

  function orderActionMeta(order) {
    const flow = String(order && order.flow || "");
    const side = String(order && order.side || "").toUpperCase();
    const action = String(order && order.position_action || "").trim().toLowerCase();
    const actionLabel = String(order && order.position_action_label || "").trim();
    if (action && actionLabel) return [action, actionLabel];
    if (flow === "exit") {
      if (action === "trim") return ["trim", actionLabel || "部分减仓"];
      if (action === "exit") return ["exit", actionLabel || "完全退出"];
      return ["exit", actionLabel || String(order && order.exit_label || "退出")];
    }
    if (action === "add") return ["add", actionLabel || "追加买入"];
    if (action === "entry") return ["entry", actionLabel || "首次入场"];
    if (side === "BUY") return ["entry", actionLabel || "买入"];
    if (side === "SELL") return ["exit", actionLabel || "卖出"];
    return [action || side.toLowerCase(), actionLabel || side || "事件"];
  }

  function actionTagMeta(action, label) {
    const value = String(action || "").trim().toLowerCase();
    if (value === "entry") return ["ok", label || "首次入场"];
    if (value === "add") return ["wait", label || "追加买入"];
    if (value === "trim") return ["cancel", label || "部分减仓"];
    if (value === "exit") return ["danger", label || "完全退出"];
    return ["wait", label || "事件"];
  }

  function signalStatusMeta(status) {
    const value = String(status || "").trim().toLowerCase();
    if (value === "filled") return ["ok", "已成交"];
    if (value === "risk_rejected") return ["danger", "风控拒绝"];
    if (value === "order_rejected") return ["danger", "下单拒绝"];
    if (value === "duplicate_skipped") return ["cancel", "重复跳过"];
    if (value === "skipped") return ["cancel", "已跳过"];
    if (value === "candidate") return ["wait", "候选"];
    return ["wait", value || "未知"];
  }

  function traceStatusMeta(status) {
    const value = String(status || "").trim().toLowerCase();
    if (value === "closed") return ["cancel", "已关闭"];
    return ["ok", "进行中"];
  }

  function sourceWalletLabel(value) {
    const raw = String(value || "").trim();
    if (!raw) return "未标记来源";
    if (raw === "system-time-exit") return "系统时间退出";
    if (raw === "system-emergency-stop") return "系统紧急退出";
    return shortWallet(raw);
  }

  function attrToken(value) {
    return encodeURIComponent(String(value || ""));
  }

  function readAttrToken(value) {
    try {
      return decodeURIComponent(String(value || ""));
    } catch (_err) {
      return String(value || "");
    }
  }

  function exitSampleKey(sample) {
    return [
      String(sample && sample.ts || 0),
      String(sample && sample.token_id || ""),
      String(sample && sample.title || ""),
      String(sample && sample.exit_kind || ""),
      String(sample && sample.status || ""),
    ].join("::");
  }

  function pendingOrderRowKey(row, index = 0) {
    return [
      String(row && row.key || ""),
      String(row && row.order_id || ""),
      String(row && row.trace_id || ""),
      String(row && row.token_id || ""),
      String(row && row.side || ""),
      String(index),
    ].join("::");
  }

  function pendingOrderStatusMeta(status) {
    const value = String(status || "").trim().toLowerCase();
    if (value === "filled" || value === "matched" || value === "confirmed") return ["ok", "FILLED"];
    if (value === "partially_filled" || value === "partial_fill" || value === "delayed") return ["wait", "PARTIAL"];
    if (value === "live" || value === "pending" || value === "submitted" || value === "accepted") return ["wait", "LIVE"];
    if (value === "canceled" || value === "cancelled" || value === "unmatched") return ["cancel", "CANCELED"];
    if (value === "failed" || value === "rejected" || value === "error") return ["danger", "FAILED"];
    return ["cancel", String(status || "UNKNOWN").toUpperCase() || "UNKNOWN"];
  }

  function operatorActionMeta(action) {
    const status = String(action && action.status || "").trim().toLowerCase();
    const cleared = Number(action && action.cleared_count || 0);
    if (status === "requested") return { cls: "wait", label: "REQUESTED", valueCls: "warn" };
    if (status === "cleared") return { cls: "ok", label: cleared > 0 ? `CLEARED ${cleared}` : "CLEARED", valueCls: "value-positive" };
    if (status === "noop") return { cls: "cancel", label: "NOOP", valueCls: "value-neutral" };
    return { cls: "cancel", label: "IDLE", valueCls: "value-neutral" };
  }

  function selectDefaultDiagnosticFocus(startupChecks, pendingOrders) {
    const problematicStartup = startupChecks.find((row) => {
      const status = String(row && row.status || "").trim().toUpperCase();
      return status === "FAIL" || status === "WARN";
    });
    if (problematicStartup) {
      return { kind: "startup", key: String(problematicStartup.name || "startup") };
    }
    if (pendingOrders.length > 0) {
      return { kind: "pending", key: String(pendingOrders[0]._diagKey || "") };
    }
    if (startupChecks.length > 0) {
      return { kind: "startup", key: String(startupChecks[0].name || "startup") };
    }
    return { kind: "", key: "" };
  }

  function syncExitReviewFilter(review) {
    const byKind = Array.isArray(review && review.by_kind) ? review.by_kind : [];
    const byTopic = Array.isArray(review && review.by_topic) ? review.by_topic : [];
    const bySource = Array.isArray(review && review.by_source) ? review.by_source : [];
    const recent = Array.isArray(review && review.recent_exits) ? review.recent_exits : [];

    const hasKind = !exitReviewFilter.kind || byKind.some((item) => String(item.exit_kind || item.key || "") === exitReviewFilter.kind)
      || recent.some((item) => String(item.exit_kind || "") === exitReviewFilter.kind);
    const hasTopic = !exitReviewFilter.topic || byTopic.some((item) => String(item.topic_label || item.key || "") === exitReviewFilter.topic)
      || recent.some((item) => String(item.topic_label || "") === exitReviewFilter.topic);
    const hasSource = !exitReviewFilter.source || bySource.some((item) => String(item.source_wallet || item.key || "") === exitReviewFilter.source)
      || recent.some((item) => String(item.source_wallet || "") === exitReviewFilter.source);

    if (!hasKind) exitReviewFilter.kind = "";
    if (!hasTopic) exitReviewFilter.topic = "";
    if (!hasSource) exitReviewFilter.source = "";
  }

  function exitFilterStateText(review) {
    const labels = [];
    const byKind = Array.isArray(review && review.by_kind) ? review.by_kind : [];
    const byTopic = Array.isArray(review && review.by_topic) ? review.by_topic : [];
    const bySource = Array.isArray(review && review.by_source) ? review.by_source : [];
    if (exitReviewFilter.kind) {
      const selected = byKind.find((item) => String(item.exit_kind || item.key || "") === exitReviewFilter.kind);
      labels.push(selected ? String(selected.label || "退出") : exitReviewFilter.kind);
    }
    if (exitReviewFilter.topic) {
      const selected = byTopic.find((item) => String(item.topic_label || item.key || "") === exitReviewFilter.topic);
      labels.push(selected ? String(selected.label || "未标记题材") : exitReviewFilter.topic);
    }
    if (exitReviewFilter.source) {
      const selected = bySource.find((item) => String(item.source_wallet || item.key || "") === exitReviewFilter.source);
      labels.push(selected ? sourceWalletLabel(selected.source_wallet || selected.key) : sourceWalletLabel(exitReviewFilter.source));
    }
    return labels.length > 0 ? `筛选: ${labels.join(" / ")}` : "全部退出样本";
  }

  function renderExitReview(review, now) {
    lastExitReview = review || {};
    lastExitReviewNow = now;
    const metaEl = $("exit-review-meta");
    const summaryEl = $("exit-review-summary");
    const kindEl = $("exit-review-kind");
    const topicEl = $("exit-review-topic");
    const sourceEl = $("exit-review-source");
    const filterStateEl = $("exit-review-filter-state");
    const filterResetEl = $("exit-review-filter-reset");
    const samplesMetaEl = $("exit-review-samples-meta");
    const samplesBodyEl = $("exit-review-samples-body");
    const detailMetaEl = $("exit-review-detail-meta");
    const detailHeadEl = $("exit-review-detail-head");
    const detailSummaryEl = $("exit-review-detail-summary");
    const detailListEl = $("exit-review-detail-list");
    const detailChainMetaEl = $("exit-review-detail-chain-meta");
    const detailChainEl = $("exit-review-detail-chain");
    if (!metaEl || !summaryEl || !kindEl || !topicEl || !sourceEl || !filterStateEl || !filterResetEl || !samplesMetaEl || !samplesBodyEl || !detailMetaEl || !detailHeadEl || !detailSummaryEl || !detailListEl || !detailChainMetaEl || !detailChainEl) return;

    const summary = (review && review.summary) || {};
    const total = Number(summary.total_exit_orders || 0);
    const filled = Number(summary.filled_exit_orders || 0);
    const rejected = Number(summary.rejected_exit_orders || 0);
    const totalNotional = Number(summary.total_notional || 0);
    const latestExitTs = Number(summary.latest_exit_ts || 0);
    const topics = Number(summary.topics || 0);
    const sources = Number(summary.sources || 0);
    const avgHoldMinutes = Number(summary.avg_hold_minutes || 0);
    const maxHoldMinutes = Number(summary.max_hold_minutes || 0);
    metaEl.textContent = total > 0
      ? `recent ${total} exits · ${topics} topics · ${sources} sources`
      : "recent 0 exits";

    summaryEl.innerHTML = [
      `<div class="review-chip"><span>最近退出</span><b>${total}</b></div>`,
      `<div class="review-chip"><span>已成交</span><b>${filled}</b></div>`,
      `<div class="review-chip"><span>已拒绝</span><b>${rejected}</b></div>`,
      `<div class="review-chip"><span>退出金额</span><b>${fmtUsd(totalNotional, false)}</b></div>`,
      `<div class="review-chip"><span>平均持有</span><b>${avgHoldMinutes > 0 ? fmtHoldMinutes(avgHoldMinutes) : "--"}</b></div>`,
      `<div class="review-chip"><span>最长持有</span><b>${maxHoldMinutes > 0 ? fmtHoldMinutes(maxHoldMinutes) : "--"}</b></div>`,
    ].join("");

    syncExitReviewFilter(review);
    filterStateEl.textContent = exitFilterStateText(review);
    filterResetEl.disabled = !(exitReviewFilter.kind || exitReviewFilter.topic || exitReviewFilter.source);

    const renderReviewList = (el, items, labelOf, detailOf, emptyText, filterType, valueOf) => {
      const rows = Array.isArray(items) ? items : [];
      el.innerHTML = rows.length > 0
        ? rows.map((item) => {
            const label = labelOf(item);
            const detail = detailOf(item);
            const rawValue = String(valueOf(item) || "");
            const active = rawValue && exitReviewFilter[filterType] === rawValue;
            return `<li class="review-selectable${active ? " active" : ""}" data-filter-type="${filterType}" data-filter-value="${attrToken(rawValue)}">
              <div class="review-main">
                <span>${label}</span>
                <b>${Number(item.filled_count || 0)}F / ${Number(item.rejected_count || 0)}R</b>
              </div>
              <div class="review-sub">${detail}</div>
            </li>`;
          }).join("")
        : `<li><div class="review-main"><span>${emptyText}</span><b>--</b></div></li>`;
    };

    renderReviewList(
      kindEl,
      review && review.by_kind,
      (item) => String(item.label || "退出"),
      (item) => `${fmtUsd(item.notional || 0, false)} · ${Number(item.count || 0)} 次 · avg ${Number(item.avg_hold_minutes || 0) > 0 ? fmtHoldMinutes(item.avg_hold_minutes) : "--"} · ${Number(item.latest_ts || 0) > 0 ? historyAgeLabel(item.latest_ts, now) : "未记录"}`,
      "暂无退出类型统计",
      "kind",
      (item) => String(item.exit_kind || item.key || "")
    );
    renderReviewList(
      topicEl,
      review && review.by_topic,
      (item) => String(item.label || "未标记题材"),
      (item) => `${fmtUsd(item.notional || 0, false)} · ${Number(item.count || 0)} 次 · avg ${Number(item.avg_hold_minutes || 0) > 0 ? fmtHoldMinutes(item.avg_hold_minutes) : "--"} · ${Number(item.latest_ts || 0) > 0 ? historyAgeLabel(item.latest_ts, now) : "未记录"}`,
      "暂无题材统计",
      "topic",
      (item) => String(item.topic_label || item.key || "")
    );
    renderReviewList(
      sourceEl,
      review && review.by_source,
      (item) => sourceWalletLabel(item.source_wallet || item.key),
      (item) => `${fmtUsd(item.notional || 0, false)} · ${Number(item.count || 0)} 次 · avg ${Number(item.avg_hold_minutes || 0) > 0 ? fmtHoldMinutes(item.avg_hold_minutes) : "--"} · ${Number(item.latest_ts || 0) > 0 ? historyAgeLabel(item.latest_ts, now) : "未记录"}`,
      "暂无来源统计",
      "source",
      (item) => String(item.source_wallet || item.key || "")
    );

    const samples = Array.isArray(review && review.recent_exits) ? review.recent_exits : [];
    const filteredSamples = samples.filter((sample) => {
      if (exitReviewFilter.kind && String(sample.exit_kind || "") !== exitReviewFilter.kind) return false;
      if (exitReviewFilter.topic && String(sample.topic_label || "") !== exitReviewFilter.topic) return false;
      if (exitReviewFilter.source && String(sample.source_wallet || "") !== exitReviewFilter.source) return false;
      return true;
    });
    if (!filteredSamples.some((sample) => exitSampleKey(sample) === selectedExitSampleKey)) {
      selectedExitSampleKey = filteredSamples[0] ? exitSampleKey(filteredSamples[0]) : "";
    }
    samplesMetaEl.textContent = `${filteredSamples.length} / ${samples.length} samples`;
    samplesBodyEl.innerHTML = filteredSamples.length > 0
      ? filteredSamples.map((sample) => {
          const sampleKey = exitSampleKey(sample);
          const [exitCls, exitTag] = exitTagMeta(sample.exit_kind, sample.exit_label);
          const [resultCls, resultTag] = exitResultMeta(sample.exit_result, sample.exit_result_label);
          const status = String(sample.status || "").toUpperCase();
          const [statusCls, statusText] = status === "FILLED"
            ? ["ok", "已成交"]
            : status === "REJECTED"
              ? ["danger", "已拒绝"]
              : status === "PENDING"
                ? ["wait", "待成交"]
                : ["cancel", status || "未知"];
          const summaryText = String(sample.exit_summary || "").trim();
          return `<tr>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main mono">${hhmm(Number(sample.ts || now))}</span>
                <span class="cell-sub">${Number(sample.ts || 0) > 0 ? historyAgeLabel(sample.ts, now) : "未记录"}</span>
              </div>
            </td>
            <td>${sample.title || "-"}</td>
            <td class="wrap">
              <div class="cell-stack">
                <span><span class="tag ${exitCls}">${exitTag}</span> <span class="tag ${resultCls}">${resultTag}</span> <span class="tag ${statusCls}">${statusText}</span></span>
                <span class="cell-sub">${sample.exit_kind || "exit"} · 持有 ${Number(sample.hold_minutes || 0) > 0 ? fmtHoldMinutes(sample.hold_minutes) : "--"}</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${sourceWalletLabel(sample.source_wallet || sample.source_label)}</span>
                <span class="cell-sub">${sample.topic_label || "未标记题材"}</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${fmtUsd(sample.notional || 0, false)}</span>
                <span class="cell-sub">${summaryText || "暂无摘要"}</span>
              </div>
            </td>
          </tr>`.replace("<tr>", `<tr class="click-row${sampleKey === selectedExitSampleKey ? " active-row" : ""}" data-exit-sample-key="${attrToken(sampleKey)}" data-trace-id="${attrToken(sample.trace_id || sample.current_position && sample.current_position.trace_id || "")}">`);
        }).join("")
      : '<tr><td colspan="5">当前筛选下暂无退出样本</td></tr>';

    const selectedSample = filteredSamples.find((sample) => exitSampleKey(sample) === selectedExitSampleKey) || filteredSamples[0] || null;
    if (!selectedSample) {
      detailMetaEl.textContent = "未选择";
      detailHeadEl.innerHTML = '<span class="tag danger">等待</span><span class="mono">点击退出样本查看详情</span>';
      detailSummaryEl.textContent = "这里会展示单条退出样本的来源钱包、题材、当前持仓上下文和最近一次退出记录。";
      detailListEl.innerHTML = "<li><span>状态</span><b>等待数据...</b></li>";
      detailChainMetaEl.textContent = "0 events";
      detailChainEl.innerHTML = '<li><span>--:--</span><b>等待数据...</b></li>';
    } else {
      const [exitCls, exitTag] = exitTagMeta(selectedSample.exit_kind, selectedSample.exit_label);
      const [resultCls, resultTag] = exitResultMeta(selectedSample.exit_result, selectedSample.exit_result_label);
      const status = String(selectedSample.status || "").toUpperCase();
      const [statusCls, statusText] = status === "FILLED"
        ? ["ok", "已成交"]
        : status === "REJECTED"
          ? ["danger", "已拒绝"]
          : status === "PENDING"
            ? ["wait", "待成交"]
            : ["cancel", status || "未知"];
      const position = selectedSample.current_position || {};
      const currentOpen = !!position.is_open;
      const entryWallet = String(position.entry_wallet || selectedSample.entry_wallet || "");
      const entryTier = String(position.entry_wallet_tier || selectedSample.entry_wallet_tier || "");
      const entryScore = Number(position.entry_wallet_score || selectedSample.entry_wallet_score || 0);
      const entryTopic = String(position.entry_topic_label || selectedSample.entry_topic_label || selectedSample.topic_label || "");
      const entryTopicSummary = String(position.entry_topic_summary || selectedSample.entry_topic_summary || "").trim();
      const entryReason = String(position.entry_reason || selectedSample.entry_reason || "").trim();
      const detailSummary = String(selectedSample.exit_summary || selectedSample.reason || "").trim();
      const eventChain = Array.isArray(selectedSample.event_chain) ? selectedSample.event_chain : [];
      detailMetaEl.textContent = `${hhmm(Number(selectedSample.ts || now))} · ${exitTag}`;
      detailHeadEl.innerHTML =
        `<span class="tag ${exitCls}">${exitTag}</span>` +
        `<span class="tag ${resultCls}">${resultTag}</span>` +
        `<span class="tag ${statusCls}">${statusText}</span>` +
        `<span class="tag ${currentOpen ? "wait" : "cancel"}">${currentOpen ? "仍有持仓" : "已无持仓"}</span>`;
      detailSummaryEl.textContent = detailSummary || "暂无退出摘要";
      detailListEl.innerHTML = [
        `<li><span>市场 / 方向</span><b>${selectedSample.title || "-"} · ${selectedSample.outcome || "--"}</b></li>`,
        `<li><span>来源钱包</span><b>${sourceWalletLabel(selectedSample.source_wallet || selectedSample.source_label)}${selectedSample.wallet_tier ? ` · ${selectedSample.wallet_tier}` : ""}${Number(selectedSample.wallet_score || 0) > 0 ? ` · ${Number(selectedSample.wallet_score || 0).toFixed(1)}` : ""}</b></li>`,
        `<li><span>入场上下文</span><b>${entryWallet ? `${sourceWalletLabel(entryWallet)}${entryTier ? ` · ${entryTier}` : ""}${entryScore > 0 ? ` · ${entryScore.toFixed(1)}` : ""}` : "暂无 entry wallet"}${entryTopic ? ` · ${entryTopic}` : ""}</b></li>`,
        `<li><span>入场原因</span><b>${entryReason || entryTopicSummary || "暂无入场说明"}</b></li>`,
        `<li><span>回放 Trace</span><b>${selectedSample.trace_id || position.trace_id || "--"}</b></li>`,
        `<li><span>持有时长</span><b>${Number(selectedSample.hold_minutes || 0) > 0 ? fmtHoldMinutes(selectedSample.hold_minutes) : "--"}</b></li>`,
        `<li><span>当前仓位</span><b>${currentOpen ? `${fmtUsd(position.notional || 0, false)} / ${Number(position.quantity || 0).toFixed(2)}份` : "当前无持仓，可能已退出完成"}</b></li>`,
        `<li><span>最近退出记录</span><b>${position.last_exit_label ? `${position.last_exit_label}${position.last_exit_summary ? ` · ${position.last_exit_summary}` : ""}` : detailSummary || "暂无记录"}</b></li>`,
      ].join("");
      detailChainMetaEl.textContent = `${eventChain.length} events`;
      detailChainEl.innerHTML = eventChain.length > 0
        ? eventChain.map((item) => {
            const [itemCls, itemTag] = item.flow === "exit"
              ? exitTagMeta(item.exit_kind || "", item.action_label || "")
              : ["ok", item.action_label || "买入"];
            const [itemResultCls, itemResultTag] = item.flow === "exit"
              ? exitResultMeta(item.exit_result, item.exit_result_label)
              : ["ok", "入场"];
            return `<li>
              <span>${hhmm(Number(item.ts || now))}</span>
              <b><span class="tag ${itemCls}">${itemTag}</span> <span class="tag ${itemResultCls}">${itemResultTag}</span> ${fmtUsd(item.notional || 0, false)}${Number(item.hold_minutes || 0) > 0 ? ` · 持有 ${fmtHoldMinutes(item.hold_minutes)}` : ""}</b>
              <div class="cell-sub">${String(item.reason || "暂无说明")}</div>
            </li>`;
          }).join("")
        : '<li><span>--:--</span><b>暂无同 token 事件链</b></li>';
    }

    if (total <= 0 && latestExitTs <= 0) {
      metaEl.textContent = "recent 0 exits";
    }
    persistExitReviewUiState();
  }

  function renderSignalReview(review, now) {
    lastSignalReview = review || {};
    lastSignalReviewNow = now;
    const metaEl = $("trace-review-meta");
    const summaryEl = $("trace-review-summary");
    const cyclesEl = $("trace-review-cycles");
    const cycleMetaEl = $("trace-review-cycle-meta");
    const cycleBodyEl = $("trace-review-cycle-body");
    const detailMetaEl = $("trace-review-detail-meta");
    const detailHeadEl = $("trace-review-detail-head");
    const detailSummaryEl = $("trace-review-detail-summary");
    const detailListEl = $("trace-review-detail-list");
    const detailChainMetaEl = $("trace-review-detail-chain-meta");
    const detailChainEl = $("trace-review-detail-chain");
    if (!metaEl || !summaryEl || !cyclesEl || !cycleMetaEl || !cycleBodyEl || !detailMetaEl || !detailHeadEl || !detailSummaryEl || !detailListEl || !detailChainMetaEl || !detailChainEl) return;

    const summary = review && review.summary || {};
    const cycles = Array.isArray(review && review.cycles) ? review.cycles : [];
    const traces = Array.isArray(review && review.traces) ? review.traces : [];
    metaEl.textContent = `recent ${Number(summary.cycles || 0)} cycles · ${Number(summary.traces || 0)} traces`;
    summaryEl.innerHTML = [
      `<div class="review-chip"><span>最近轮次</span><b>${Number(summary.cycles || 0)}</b></div>`,
      `<div class="review-chip"><span>候选信号</span><b>${Number(summary.candidates || 0)}</b></div>`,
      `<div class="review-chip"><span>已成交</span><b>${Number(summary.filled || 0)}</b></div>`,
      `<div class="review-chip"><span>已拒绝</span><b>${Number(summary.rejected || 0)}</b></div>`,
      `<div class="review-chip"><span>活跃 Trace</span><b>${Number(summary.open_traces || 0)}</b></div>`,
      `<div class="review-chip"><span>已关闭 Trace</span><b>${Number(summary.closed_traces || 0)}</b></div>`,
    ].join("");

    if (!traces.some((trace) => String(trace.trace_id || "") === selectedTraceId)) {
      selectedTraceId = traces[0] ? String(traces[0].trace_id || "") : "";
    }
    const selectedTrace = traces.find((trace) => String(trace.trace_id || "") === selectedTraceId) || traces[0] || null;
    if (!cycles.some((cycle) => String(cycle.cycle_id || "") === selectedSignalCycleId)) {
      const traceCycle = selectedTrace && Array.isArray(selectedTrace.decision_chain) && selectedTrace.decision_chain.length > 0
        ? String(selectedTrace.decision_chain[selectedTrace.decision_chain.length - 1].cycle_id || "")
        : "";
      selectedSignalCycleId = traceCycle || (cycles[0] ? String(cycles[0].cycle_id || "") : "");
    }
    const selectedCycle = cycles.find((cycle) => String(cycle.cycle_id || "") === selectedSignalCycleId) || cycles[0] || null;

    cyclesEl.innerHTML = cycles.length > 0
      ? cycles.map((cycle) => {
          const active = String(cycle.cycle_id || "") === selectedSignalCycleId;
          const preview = Array.isArray(cycle.wallet_pool_preview) ? cycle.wallet_pool_preview : [];
          const previewText = preview.length > 0
            ? preview.map((item) => `${shortWallet(item.wallet)} ${Number(item.wallet_score || 0).toFixed(1)}`).join(" / ")
            : "wallet pool snapshot";
          return `<li class="review-selectable${active ? " active" : ""}" data-signal-cycle-id="${attrToken(cycle.cycle_id || "")}">
            <div class="review-main">
              <span>${hhmm(Number(cycle.ts || now))}</span>
              <b>${Number(cycle.candidate_count || 0)} sig</b>
            </div>
            <div class="review-sub">${Number(cycle.filled_count || 0)} filled / ${Number(cycle.rejected_count || 0)} reject / ${Number(cycle.skipped_count || 0)} skip · ${previewText}</div>
          </li>`;
        }).join("")
      : '<li><div class="review-main"><span>暂无 signal cycles</span><b>--</b></div></li>';

    const cycleCandidates = Array.isArray(selectedCycle && selectedCycle.candidates) ? selectedCycle.candidates : [];
    cycleMetaEl.textContent = `${cycleCandidates.length} candidates`;
    cycleBodyEl.innerHTML = cycleCandidates.length > 0
      ? cycleCandidates.map((candidate) => {
          const [actionCls, actionTag] = actionTagMeta(candidate.action, candidate.action_label);
          const [statusCls, statusTag] = signalStatusMeta(candidate.final_status);
          const walletPoolPreview = Array.isArray(candidate.wallet_pool_preview) ? candidate.wallet_pool_preview : [];
          const previewText = walletPoolPreview.length > 0
            ? walletPoolPreview.map((item) => `${shortWallet(item.wallet)} ${Number(item.wallet_score || 0).toFixed(0)}`).join(" / ")
            : "无钱包池快照";
          const traceId = String(candidate.trace_id || "");
          const rowClass = traceId && traceId === selectedTraceId ? "click-row active-row" : "click-row";
          const decisionReason = String(candidate.decision_reason || "").trim();
          const orderReason = String(candidate.order_reason || "").trim();
          return `<tr>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${candidate.title || "-"}</span>
                <span class="cell-sub">${candidate.outcome || "--"} · ${candidate.topic_label || "未标记题材"}</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${shortWallet(candidate.wallet)}</span>
                <span class="cell-sub">${candidate.wallet_tier || "-"} · ${Number(candidate.wallet_score || 0).toFixed(1)}</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span><span class="tag ${actionCls}">${actionTag}</span> <span class="tag ${statusCls}">${statusTag}</span></span>
                <span class="cell-sub">${candidate.topic_bias || "neutral"} x${Number(candidate.topic_multiplier || 1).toFixed(2)}</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${decisionReason || "通过"}</span>
                <span class="cell-sub">${Number(candidate.final_notional || 0) > 0 ? `${fmtUsd(candidate.final_notional || 0, false)} / ` : ""}${previewText}</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${orderReason || (candidate.order_status || "未下单")}</span>
                <span class="cell-sub">${candidate.order_status || "candidate"}${Number(candidate.order_notional || 0) > 0 ? ` · ${fmtUsd(candidate.order_notional || 0, false)}` : ""}</span>
              </div>
            </td>
          </tr>`.replace("<tr>", `<tr class="${rowClass}" data-trace-id="${attrToken(traceId)}" data-signal-cycle-id="${attrToken(selectedCycle && selectedCycle.cycle_id || "")}">`);
        }).join("")
      : '<tr><td colspan="5">当前轮次暂无候选信号</td></tr>';

    if (!selectedTrace) {
      detailMetaEl.textContent = "未选择";
      detailHeadEl.innerHTML = '<span class="tag danger">等待</span><span class="mono">点击持仓、退出样本或候选信号查看完整链路</span>';
      detailSummaryEl.textContent = "这里会展示同一条 trace 从首次入场到追加、减仓、退出的完整决策链。";
      detailListEl.innerHTML = "<li><span>状态</span><b>等待数据...</b></li>";
      detailChainMetaEl.textContent = "0 events";
      detailChainEl.innerHTML = '<li><span>--:--</span><b>等待数据...</b></li>';
      persistExitReviewUiState();
      return;
    }

    const [traceCls, traceTag] = traceStatusMeta(selectedTrace.status);
    const currentPosition = selectedTrace.current_position || {};
    const decisionChain = Array.isArray(selectedTrace.decision_chain) ? selectedTrace.decision_chain : [];
    const latestStep = decisionChain[decisionChain.length - 1] || {};
    const [latestActionCls, latestActionTag] = actionTagMeta(selectedTrace.latest_action || latestStep.action, selectedTrace.latest_action_label || latestStep.action_label);
    const latestStatus = String(selectedTrace.latest_order_status || latestStep.order_status || "").toUpperCase();
    const [latestStatusCls, latestStatusTag] = latestStatus === "FILLED"
      ? ["ok", "已成交"]
      : latestStatus === "REJECTED"
        ? ["danger", "已拒绝"]
        : latestStatus
          ? ["wait", latestStatus]
          : ["cancel", "无订单"];
    detailMetaEl.textContent = `${selectedTrace.trace_id || "trace"} · ${selectedTrace.market_slug || "-"}`;
    detailHeadEl.innerHTML =
      `<span class="tag ${traceCls}">${traceTag}</span>` +
      `<span class="tag ${latestActionCls}">${latestActionTag}</span>` +
      `<span class="tag ${latestStatusCls}">${latestStatusTag}</span>` +
      `<span class="mono">${selectedTrace.entry_signal_id || "--"} -> ${selectedTrace.last_signal_id || "--"}</span>`;
    detailSummaryEl.textContent = String(selectedTrace.entry_reason || latestStep.order_reason || latestStep.skip_reason || latestStep.risk_reason || "暂无入场说明");
    detailListEl.innerHTML = [
      `<li><span>市场 / 方向</span><b>${selectedTrace.market_slug || "-"} · ${selectedTrace.outcome || "--"}</b></li>`,
      `<li><span>首次入场</span><b>${selectedTrace.entry_wallet ? `${sourceWalletLabel(selectedTrace.entry_wallet)}${selectedTrace.entry_wallet_tier ? ` · ${selectedTrace.entry_wallet_tier}` : ""}${Number(selectedTrace.entry_wallet_score || 0) > 0 ? ` · ${Number(selectedTrace.entry_wallet_score || 0).toFixed(1)}` : ""}` : "暂无 entry wallet"}${selectedTrace.entry_topic_label ? ` · ${selectedTrace.entry_topic_label}` : ""}</b></li>`,
      `<li><span>当前仓位</span><b>${currentPosition && currentPosition.trace_id ? `${fmtUsd(currentPosition.notional || 0, false)} / ${Number(currentPosition.quantity || 0).toFixed(2)}份` : "当前无持仓"}</b></li>`,
      `<li><span>最近动作</span><b>${latestActionTag}${latestStep.order_reason ? ` · ${latestStep.order_reason}` : latestStep.skip_reason ? ` · ${latestStep.skip_reason}` : latestStep.risk_reason ? ` · ${latestStep.risk_reason}` : ""}</b></li>`,
      `<li><span>开仓时间</span><b>${Number(selectedTrace.opened_ts || 0) > 0 ? hhmm(selectedTrace.opened_ts) : "--"}${Number(selectedTrace.closed_ts || 0) > 0 ? ` / 关闭 ${hhmm(selectedTrace.closed_ts)}` : ""}</b></li>`,
      `<li><span>当前状态</span><b>${traceTag}${currentPosition && currentPosition.last_exit_label ? ` · ${currentPosition.last_exit_label}` : ""}</b></li>`,
    ].join("");
    detailChainMetaEl.textContent = `${decisionChain.length} events`;
    detailChainEl.innerHTML = decisionChain.length > 0
      ? decisionChain.map((item) => {
          const [actionCls, actionTag] = actionTagMeta(item.action, item.action_label);
          const [statusCls, statusTag] = signalStatusMeta(item.final_status || item.order_status);
          const reasonText = String(item.order_reason || item.skip_reason || item.risk_reason || "").trim();
          const snapshotText = Number(item.position_notional || 0) > 0
            ? `${fmtUsd(item.position_notional || 0, false)} / ${Number(item.position_quantity || 0).toFixed(2)}份`
            : `${fmtUsd(item.final_notional || 0, false)}`;
          const poolPreview = Array.isArray(item.wallet_pool_preview) && item.wallet_pool_preview.length > 0
            ? item.wallet_pool_preview.map((row) => `${shortWallet(row.wallet)} ${Number(row.wallet_score || 0).toFixed(0)}`).join(" / ")
            : "";
          return `<li>
            <span>${Number(item.ts || 0) > 0 ? hhmm(item.ts) : "--:--"} · ${item.cycle_id || "--"} · ${item.signal_id || "--"}</span>
            <b><span class="tag ${actionCls}">${actionTag}</span> <span class="tag ${statusCls}">${statusTag}</span> ${snapshotText}</b>
            <div class="cell-sub">${item.wallet ? `${shortWallet(item.wallet)} · ` : ""}${item.topic_label ? `${item.topic_label} · ` : ""}${reasonText || "暂无说明"}${poolPreview ? ` · pool ${poolPreview}` : ""}</div>
          </li>`;
        }).join("")
      : '<li><span>--:--</span><b>暂无决策链</b></li>';

    persistExitReviewUiState();
  }

  function latestCycleIdForTrace(review, traceId) {
    const traces = Array.isArray(review && review.traces) ? review.traces : [];
    const trace = traces.find((item) => String(item.trace_id || "") === String(traceId || ""));
    if (!trace || !Array.isArray(trace.decision_chain) || trace.decision_chain.length <= 0) return "";
    return String(trace.decision_chain[trace.decision_chain.length - 1].cycle_id || "");
  }

  function renderAttributionReview(review, now) {
    lastAttributionReview = review || {};
    lastAttributionReviewNow = now;
    const metaEl = $("attribution-review-meta");
    const windowMetaEl = $("attribution-window-meta");
    const summaryEl = $("attribution-review-summary");
    const chipsEl = $("attribution-window-chips");
    const byWalletEl = $("attr-by-wallet");
    const byTopicEl = $("attr-by-topic");
    const byExitKindEl = $("attr-by-exit-kind");
    const walletTopicMetaEl = $("attr-wallet-topic-meta");
    const walletTopicBodyEl = $("attr-wallet-topic-body");
    const topicExitMetaEl = $("attr-topic-exit-meta");
    const topicExitBodyEl = $("attr-topic-exit-body");
    const sourceResultMetaEl = $("attr-source-result-meta");
    const sourceResultBodyEl = $("attr-source-result-body");
    const rejectReasonsEl = $("attr-reject-reasons");
    const holdBucketsEl = $("attr-hold-buckets");
    const topWalletsEl = $("attr-top-wallets");
    const bottomWalletsEl = $("attr-bottom-wallets");
    const topTopicsEl = $("attr-top-topics");
    const bottomTopicsEl = $("attr-bottom-topics");
    if (!metaEl || !windowMetaEl || !summaryEl || !chipsEl || !byWalletEl || !byTopicEl || !byExitKindEl || !walletTopicMetaEl || !walletTopicBodyEl || !topicExitMetaEl || !topicExitBodyEl || !sourceResultMetaEl || !sourceResultBodyEl || !rejectReasonsEl || !holdBucketsEl || !topWalletsEl || !bottomWalletsEl || !topTopicsEl || !bottomTopicsEl) return;

    const summary = review && review.summary || {};
    const windows = review && review.windows || {};
    const windowKeys = Array.isArray(summary.windows) ? summary.windows : Object.keys(windows);
    if (!windowKeys.includes(selectedAttributionWindow)) {
      selectedAttributionWindow = windowKeys[0] || "24h";
    }
    const current = windows[selectedAttributionWindow] || windows[windowKeys[0]] || { summary: {} };
    const currentSummary = current.summary || {};

    metaEl.textContent = `ledger ${Number(summary.available_orders || 0)} orders · ${Number(summary.available_exits || 0)} exits`;
    windowMetaEl.textContent = `${String(current.label || selectedAttributionWindow)} · ${Number(currentSummary.order_count || 0)} orders`;

    Array.from(chipsEl.querySelectorAll("[data-window]")).forEach((el) => {
      const isActive = String(el.getAttribute("data-window") || "") === selectedAttributionWindow;
      el.classList.toggle("active", isActive);
    });

    summaryEl.innerHTML = [
      `<div class="review-chip"><span>订单数</span><b>${Number(currentSummary.order_count || 0)}</b></div>`,
      `<div class="review-chip"><span>已成交</span><b>${Number(currentSummary.filled_count || 0)}</b></div>`,
      `<div class="review-chip"><span>已拒绝</span><b>${Number(currentSummary.rejected_count || 0)}</b></div>`,
      `<div class="review-chip"><span>退出数</span><b>${Number(currentSummary.exit_count || 0)}</b></div>`,
      `<div class="review-chip"><span>高分拒单</span><b>${Number(currentSummary.reject_high_score_count || 0)}</b></div>`,
      `<div class="review-chip"><span>题材数</span><b>${Number(currentSummary.topics || 0)}</b></div>`,
    ].join("");

    const renderAttrList = (el, items, labelFn, detailFn, emptyText) => {
      const rows = Array.isArray(items) ? items : [];
      el.innerHTML = rows.length > 0
        ? rows.map((item) => `<li><div class="review-main"><span>${labelFn(item)}</span><b>${Number(item.filled_count || 0)}F / ${Number(item.rejected_count || 0)}R</b></div><div class="review-sub">${detailFn(item)}</div></li>`).join("")
        : `<li><div class="review-main"><span>${emptyText}</span><b>--</b></div></li>`;
    };

    renderAttrList(
      byWalletEl,
      current.by_wallet,
      (item) => sourceWalletLabel(item.wallet || item.label),
      (item) => `${Number(item.entry_count || 0)} 入 / ${Number(item.exit_count || 0)} 出 · ${fmtUsd(item.filled_notional || 0, false)} · rej ${fmtRatioPct(item.reject_rate || 0, 0)}`,
      "暂无钱包归因"
    );
    renderAttrList(
      byTopicEl,
      current.by_topic,
      (item) => String(item.topic_label || item.label || "未标记题材"),
      (item) => `${Number(item.entry_count || 0)} 入 / ${Number(item.exit_count || 0)} 出 · ${fmtUsd(item.filled_notional || 0, false)} · avg ${Number(item.avg_hold_minutes || 0) > 0 ? fmtHoldMinutes(item.avg_hold_minutes) : "--"}`,
      "暂无题材归因"
    );
    renderAttrList(
      byExitKindEl,
      current.by_exit_kind,
      (item) => String(item.label || "退出"),
      (item) => `${Number(item.filled_count || 0)} 成交 / ${Number(item.rejected_count || 0)} 拒绝 · ${fmtUsd(item.filled_notional || 0, false)} · avg ${Number(item.avg_hold_minutes || 0) > 0 ? fmtHoldMinutes(item.avg_hold_minutes) : "--"}`,
      "暂无退出归因"
    );

    const walletTopicRows = Array.isArray(current.wallet_topic) ? current.wallet_topic : [];
    walletTopicMetaEl.textContent = `${walletTopicRows.length} rows`;
    walletTopicBodyEl.innerHTML = walletTopicRows.length > 0
      ? walletTopicRows.map((row) => `<tr>
          <td>${sourceWalletLabel(row.wallet || row.label)}</td>
          <td>${row.topic_label || "未标记题材"}</td>
          <td>${Number(row.filled_count || 0)} / ${Number(row.rejected_count || 0)}</td>
          <td>${fmtUsd(row.filled_notional || 0, false)}</td>
          <td>${Number(row.avg_hold_minutes || 0) > 0 ? fmtHoldMinutes(row.avg_hold_minutes) : "--"}</td>
        </tr>`).join("")
      : '<tr><td colspan="5">暂无钱包 x 题材归因</td></tr>';

    const topicExitRows = Array.isArray(current.topic_exit) ? current.topic_exit : [];
    topicExitMetaEl.textContent = `${topicExitRows.length} rows`;
    topicExitBodyEl.innerHTML = topicExitRows.length > 0
      ? topicExitRows.map((row) => `<tr>
          <td>${row.topic_label || "未标记题材"}</td>
          <td>${row.exit_label || row.label || "退出"}</td>
          <td>${Number(row.filled_count || 0)} / ${Number(row.rejected_count || 0)}</td>
          <td>${fmtUsd(row.filled_notional || 0, false)}</td>
          <td>${Number(row.avg_hold_minutes || 0) > 0 ? fmtHoldMinutes(row.avg_hold_minutes) : "--"}</td>
        </tr>`).join("")
      : '<tr><td colspan="5">暂无题材 x 退出归因</td></tr>';

    const sourceResultRows = Array.isArray(current.source_result) ? current.source_result : [];
    sourceResultMetaEl.textContent = `${sourceResultRows.length} rows`;
    sourceResultBodyEl.innerHTML = sourceResultRows.length > 0
      ? sourceResultRows.map((row) => `<tr>
          <td>${sourceWalletLabel(row.source_wallet || row.label)}</td>
          <td>${row.result_label || row.label || "-"}</td>
          <td>${Number(row.count || 0)}</td>
          <td>${fmtUsd(row.filled_notional || 0, false)}</td>
          <td>${fmtRatioPct(row.reject_rate || 0, 0)}</td>
        </tr>`).join("")
      : '<tr><td colspan="5">暂无来源 x 结果归因</td></tr>';

    renderAttrList(
      rejectReasonsEl,
      current.reject_reasons,
      (item) => String(item.reason_label || item.label || "拒单"),
      (item) => `${Number(item.rejected_count || 0)} 次 · 高分钱包 ${Number(item.high_score_rejected_count || 0)} 次 · avg ${Number(item.avg_wallet_score || 0).toFixed(1)}`,
      "暂无拒单归因"
    );
    renderAttrList(
      holdBucketsEl,
      current.hold_buckets,
      (item) => String(item.hold_label || item.label || "持有区间"),
      (item) => `${Number(item.count || 0)} 次 · avg ${Number(item.avg_hold_minutes || 0) > 0 ? fmtHoldMinutes(item.avg_hold_minutes) : "--"} · max ${Number(item.max_hold_minutes || 0) > 0 ? fmtHoldMinutes(item.max_hold_minutes) : "--"}`,
      "暂无持有时长归因"
    );

    const renderRankList = (el, rows, labelFn, detailFn, emptyText) => {
      const items = Array.isArray(rows) ? rows : [];
      el.innerHTML = items.length > 0
        ? items.map((row) => `<span>${labelFn(row)} · ${detailFn(row)}</span>`).join("")
        : `<span>${emptyText}</span>`;
    };

    const rankings = current.rankings || {};
    renderRankList(
      topWalletsEl,
      rankings.top_wallets,
      (row) => sourceWalletLabel(row.wallet || row.label),
      (row) => `${Number(row.filled_count || 0)}F · ${fmtUsd(row.filled_notional || 0, false)}`,
      "暂无排名"
    );
    renderRankList(
      bottomWalletsEl,
      rankings.bottom_wallets,
      (row) => sourceWalletLabel(row.wallet || row.label),
      (row) => `${Number(row.rejected_count || 0)}R · rej ${fmtRatioPct(row.reject_rate || 0, 0)}`,
      "暂无排名"
    );
    renderRankList(
      topTopicsEl,
      rankings.top_topics,
      (row) => String(row.topic_label || row.label || "未标记题材"),
      (row) => `${Number(row.filled_count || 0)}F · ${fmtUsd(row.filled_notional || 0, false)}`,
      "暂无排名"
    );
    renderRankList(
      bottomTopicsEl,
      rankings.bottom_topics,
      (row) => String(row.topic_label || row.label || "未标记题材"),
      (row) => `${Number(row.rejected_count || 0)}R · rej ${fmtRatioPct(row.reject_rate || 0, 0)}`,
      "暂无排名"
    );

    persistExitReviewUiState();
  }

  function renderOpsGate(gate) {
    const bannerEl = $("ops-banner");
    const tagEl = $("ops-gate-tag");
    const titleEl = $("ops-gate-title");
    const detailEl = $("ops-gate-detail");
    const checksEl = $("ops-gate-checks");
    const actionsMetaEl = $("ops-gate-actions-meta");
    const actionsEl = $("ops-gate-actions");
    const pillEl = $("ops-gate-pill");
    const liveStatusLabelEl = $("live-status-label");
    const liveDotEl = $("live-status-dot");
    const emergencyBtn = $("btn-emergency-stop");
    if (!bannerEl || !tagEl || !titleEl || !detailEl || !checksEl || !actionsMetaEl || !actionsEl || !pillEl || !liveStatusLabelEl || !liveDotEl || !emergencyBtn) return;

    const level = String(gate && gate.level || "observe");
    const levelMap = {
      ready: { cls: "ok", label: "READY", dot: "dot-ok", guard: "guard-ok" },
      observe: { cls: "wait", label: "OBSERVE", dot: "dot-wait", guard: "guard-wait" },
      escalate: { cls: "danger", label: "ESCALATE", dot: "dot-danger", guard: "guard-danger" },
      block: { cls: "danger", label: "BLOCK", dot: "dot-danger", guard: "guard-danger" },
    };
    const meta = levelMap[level] || levelMap.observe;

    bannerEl.classList.remove("ops-ok", "ops-wait", "ops-danger");
    bannerEl.classList.add(level === "ready" ? "ops-ok" : level === "observe" ? "ops-wait" : "ops-danger");

    tagEl.className = `tag ${meta.cls}`;
    tagEl.textContent = meta.label;
    titleEl.textContent = String(gate && gate.title || "运行门禁等待数据");
    detailEl.textContent = String(gate && gate.detail || "这里会把 startup、自检、monitor 和 reconciliation 的综合结论顶到最上层。");

    const items = Array.isArray(gate && gate.items) ? gate.items : [];
    const issues = Array.isArray(gate && gate.issues) ? gate.issues : [];
    const actions = Array.isArray(gate && gate.actions) ? gate.actions : [];
    checksEl.innerHTML = items.length > 0
      ? items.map((item) => `<li><span>${String(item.label || "-")}</span><b>${String(item.value || "-")}</b></li>`).join("")
      : '<li><span>状态</span><b>等待数据...</b></li>';
    if (issues.length > 0) {
      checksEl.innerHTML += issues.slice(0, 2).map((item) => `<li><span>重点问题</span><b>${String(item)}</b></li>`).join("");
    }

    actionsMetaEl.textContent = `${actions.length} actions`;
    actionsEl.innerHTML = actions.length > 0
      ? actions.slice(0, 5).map((item) => {
          const cls = String(item && item.cls || "wait");
          const tag = cls === "danger" ? "优先" : cls === "ok" ? "继续" : "观察";
          const controls = Array.isArray(item && item.controls) ? item.controls : [];
          const controlsHtml = controls.length > 0
            ? `<div class="ops-action-buttons">${controls.map((control) => `<button class="btn ghost" data-ops-action="${String(control.type || "")}" data-ops-value="${attrToken(control.value || "")}">${String(control.label || "")}</button>`).join("")}</div>`
            : "";
          return `<li><span class="tag ${cls}">${tag}</span><div class="ops-action-body"><b>${String(item && item.title || "-")}</b><p>${String(item && item.detail || "")}</p>${controlsHtml}</div></li>`;
        }).join("")
      : '<li><span class="tag wait">等待</span><div><b>等待运行门禁建议</b><p>这里会根据 startup、monitor 和 reconciliation 自动给出下一步动作。</p></div></li>';

    pillEl.className = `guard ${meta.guard}`;
    pillEl.textContent = `OPS ${meta.label}`;
    liveStatusLabelEl.textContent = meta.label;
    liveDotEl.classList.remove("dot-ok", "dot-wait", "dot-danger");
    liveDotEl.classList.add(meta.dot);
    emergencyBtn.classList.toggle("recommended", level === "block" || level === "escalate");
  }

  function renderMonitorReports(monitor30m, monitor12h, eodReport, now) {
    const metaEl = $("monitor-report-meta");
    const summaryEl = $("monitor-report-summary");
    const calloutEl = $("monitor-report-callout");
    const list30El = $("monitor-30m-list");
    const list12El = $("monitor-12h-list");
    const eodListEl = $("reconciliation-eod-list");
    const breakdownMetaEl = $("reconciliation-eod-breakdown-meta");
    const breakdownEl = $("reconciliation-eod-breakdown");
    if (!metaEl || !summaryEl || !calloutEl || !list30El || !list12El || !eodListEl || !breakdownMetaEl || !breakdownEl) return;

    const report30 = monitor30m || EMPTY_MONITOR_REPORT("monitor_30m");
    const report12 = monitor12h || EMPTY_MONITOR_REPORT("monitor_12h");
    const eod = eodReport || EMPTY_RECONCILIATION_EOD_REPORT;
    const eodStartup = eod.startup && typeof eod.startup === "object" ? eod.startup : {};
    const eodReconciliation = eod.reconciliation && typeof eod.reconciliation === "object" ? eod.reconciliation : {};
    const ledgerSummary = eod.ledger_summary && typeof eod.ledger_summary === "object" ? eod.ledger_summary : {};
    const recommendations = Array.isArray(eod.recommendations) ? eod.recommendations.filter((item) => String(item || "").trim()) : [];
    const issues = Array.isArray(eod.issues) ? eod.issues.filter((item) => String(item || "").trim()) : [];
    const fillBySource = Array.isArray(ledgerSummary.fill_by_source) ? ledgerSummary.fill_by_source : [];
    const fillBySide = Array.isArray(ledgerSummary.fill_by_side) ? ledgerSummary.fill_by_side : [];

    const startupReady = eodStartup.ready === false
      ? false
      : report30.startup_ready === false || report12.startup_ready === false
        ? false
        : eodStartup.ready === true || report30.startup_ready === true || report12.startup_ready === true;
    const [startupCls, startupTag] = startupReady === false
      ? ["danger", "NOT READY"]
      : startupReady === true
        ? ["ok", "READY"]
        : ["cancel", "UNKNOWN"];
    const [recon30Cls, recon30Tag] = reportDecisionMeta(report30.final_recommendation || report30.recommendation, report30.reconciliation_status);
    const [recon12Cls, recon12Tag] = reportDecisionMeta(report12.final_recommendation || report12.recommendation, report12.reconciliation_status);
    const [eodCls, eodTag] = reportStatusMeta(eod.status || eodReconciliation.status || "unknown");
    const internalDiff = Number(eodReconciliation.internal_vs_ledger_diff || 0);
    const fillCount = Number(ledgerSummary.fill_count || 0);
    const generatedTs = Math.max(Number(report30.generated_ts || 0), Number(report12.generated_ts || 0), Number(eod.generated_ts || 0));

    metaEl.textContent = generatedTs > 0
      ? `latest ${fmtDateTime(generatedTs)} · ${fmtAge(Math.max(0, Math.floor(Date.now() / 1000) - generatedTs))}前`
      : "30m / 12h / EOD";

    summaryEl.innerHTML = [
      `<div class="review-chip"><span>启动就绪</span><b><span class="tag ${startupCls}">${startupTag}</span></b></div>`,
      `<div class="review-chip"><span>30m</span><b><span class="tag ${recon30Cls}">${recon30Tag}</span></b></div>`,
      `<div class="review-chip"><span>12h</span><b><span class="tag ${recon12Cls}">${recon12Tag}</span></b></div>`,
      `<div class="review-chip"><span>EOD</span><b><span class="tag ${eodCls}">${eodTag}</span></b></div>`,
      `<div class="review-chip"><span>账本差异</span><b class="${clsForValue(internalDiff)}">${fmtUsd(internalDiff)}</b></div>`,
      `<div class="review-chip"><span>当日成交</span><b>${fillCount} fills / ${fmtUsd(ledgerSummary.fill_notional || 0, false)}</b></div>`,
    ].join("");

    let calloutCls = "ok";
    let calloutTag = "对齐";
    let calloutTitle = "执行与监控链路已接通";
    let calloutBody = "可以直接从 dashboard 读取 30m、12h 和 EOD 摘要，不再需要人工翻 shell 文本。";
    if (startupReady === false) {
      calloutCls = "danger";
      calloutTag = "阻断";
      calloutTitle = "启动自检未就绪";
      calloutBody = "先修复 smoke、账户或 broker 前置条件，再讨论参数或策略表现。";
    } else if (String(eod.status || "").toLowerCase() === "fail" || String(report12.reconciliation_status || "").toLowerCase() === "fail" || String(report30.reconciliation_status || "").toLowerCase() === "fail") {
      calloutCls = "danger";
      calloutTag = "对账失败";
      calloutTitle = "执行事实层存在漂移";
      calloutBody = String(report12.final_recommendation || report30.final_recommendation || recommendations[0] || "请优先检查 ledger 漂移、broker 同步和陈旧 pending 单。");
    } else if (String(eod.status || "").toLowerCase() === "warn" || String(report12.reconciliation_status || "").toLowerCase() === "warn" || String(report30.reconciliation_status || "").toLowerCase() === "warn") {
      calloutCls = "wait";
      calloutTag = "观察";
      calloutTitle = "执行层有警告，暂不适合调参数";
      calloutBody = String(report12.final_recommendation || report30.final_recommendation || recommendations[0] || "先处理 stale pending orders、snapshot age 和 reconcile age。");
    }
    calloutEl.innerHTML = `<span class="tag ${calloutCls}">${calloutTag}</span><div><b>${calloutTitle}</b><p>${calloutBody}</p></div>`;

    const renderMonitorWindow = (el, report, label) => {
      const counts = report.counts && typeof report.counts === "object" ? report.counts : {};
      const ratios = report.ratios && typeof report.ratios === "object" ? report.ratios : {};
      const reconciliation = report.reconciliation && typeof report.reconciliation === "object" ? report.reconciliation : {};
      const [sampleCls, sampleTag] = reportStatusMeta(report.sample_status || "unknown");
      const [decisionCls, decisionTag] = reportDecisionMeta(report.final_recommendation || report.recommendation, report.reconciliation_status || reconciliation.status || "unknown");
      const [reconCls, reconTag] = reportStatusMeta(report.reconciliation_status || reconciliation.status || "unknown");
      const issueText = String(report.reconciliation_issue_summary || "").trim() || "(none)";
      const generated = Number(report.generated_ts || 0);
      const exec = Number(counts.exec || 0);
      const skipMax = Number(counts.skip_max_open || 0);
      const addCd = Number(counts.skip_token_add_cooldown || 0);
      const timeExit = Number(counts.time_exit_close || 0);
      const reject = Number(counts.reject_wallet_failures || 0);
      const skipRatio = ratios.skip_max_open_per_exec == null ? "--" : fmtRatioPct(ratios.skip_max_open_per_exec, 0);
      const exitRatio = ratios.time_exit_close_per_exec == null ? "--" : fmtRatioPct(ratios.time_exit_close_per_exec, 0);
      const rejectRatio = ratios.reject_wallet_failures_per_exec == null ? "--" : fmtRatioPct(ratios.reject_wallet_failures_per_exec, 0);
      const recAge = Number(reconciliation.broker_reconcile_age_seconds || 0);
      const eventAge = Number(reconciliation.broker_event_sync_age_seconds || 0);
      el.innerHTML = [
        `<li><div class="review-main"><span>${label} 窗口</span><b><span class="tag ${sampleCls}">${sampleTag}</span> <span class="tag ${decisionCls}">${decisionTag}</span></b></div><div class="review-sub">${generated > 0 ? `${fmtDateTime(generated)} · ${fmtAge(Math.max(0, Math.floor(Date.now() / 1000) - generated))}前` : "尚未生成报告"}</div></li>`,
        `<li><div class="review-main"><span>样本与计数</span><b>${exec} EXEC</b></div><div class="review-sub">skip max ${skipMax} · cooldown ${addCd} · time exit ${timeExit}${reject > 0 ? ` · reject ${reject}` : ""}</div></li>`,
        `<li><div class="review-main"><span>比例与同步</span><b><span class="tag ${reconCls}">${reconTag}</span></b></div><div class="review-sub">skip ${skipRatio} · exit ${exitRatio}${reject > 0 || ratios.reject_wallet_failures_per_exec != null ? ` · reject ${rejectRatio}` : ""} · reconcile ${recAge > 0 ? fmtAge(recAge) : "--"} · events ${eventAge > 0 ? fmtAge(eventAge) : "--"}</div></li>`,
        `<li><div class="review-main"><span>最终建议</span><b>${decisionTag}</b></div><div class="review-sub">${String(report.final_recommendation || report.recommendation || issueText || "暂无建议")}</div></li>`,
      ].join("");
    };

    renderMonitorWindow(list30El, report30, "30m");
    renderMonitorWindow(list12El, report12, "12h");

    const startupFailures = Number(eodStartup.failure_count || 0);
    const startupWarnings = Number(eodStartup.warning_count || 0);
    const stalePending = Number(eodReconciliation.stale_pending_orders || 0);
    const latestFillTs = Number(ledgerSummary.latest_ts || 0);
    const eodRecommendationText = recommendations.length > 0 ? recommendations.join(" / ") : "暂无 EOD 建议";
    eodListEl.innerHTML = [
      `<li><div class="review-main"><span>日终状态</span><b><span class="tag ${eodCls}">${eodTag}</span></b></div><div class="review-sub">${eod.day_key || "--"} · ${Number(eod.generated_ts || 0) > 0 ? fmtDateTime(eod.generated_ts) : "尚未生成"} · latest fill ${latestFillTs > 0 ? historyAgeLabel(latestFillTs, now || latestFillTs) : "未记录"}</div></li>`,
      `<li><div class="review-main"><span>盈亏与差异</span><b class="${clsForValue(ledgerSummary.realized_pnl || 0)}">${fmtUsd(ledgerSummary.realized_pnl || 0)}</b></div><div class="review-sub">internal vs ledger ${fmtUsd(eodReconciliation.internal_vs_ledger_diff || 0)} · broker floor gap ${fmtUsd(eodReconciliation.broker_floor_gap_vs_internal || 0)}</div></li>`,
      `<li><div class="review-main"><span>启动与 pending</span><b>${startupFailures} fail / ${startupWarnings} warn</b></div><div class="review-sub">stale pending ${stalePending} · open ${Number(eod.state_summary && eod.state_summary.open_positions || 0)} · tracked ${fmtUsd(eod.state_summary && eod.state_summary.tracked_notional_usd || 0, false)}</div></li>`,
      `<li><div class="review-main"><span>推荐动作</span><b>${recommendations.length}</b></div><div class="review-sub">${issues.length > 0 ? `${issues.join("; ")} · ` : ""}${eodRecommendationText}</div></li>`,
    ].join("");

    const breakdownCards = [];
    fillBySource.slice(0, 4).forEach((bucket) => {
      breakdownCards.push(`<div class="component-card">
        <span>来源 ${String(bucket.source || "unknown")}</span>
        <b>${Number(bucket.fill_count || 0)} fills</b>
        <div class="tiny-list">
          <span><i>金额</i><strong>${fmtUsd(bucket.notional || 0, false)}</strong></span>
          <span><i>已实现</i><strong class="${clsForValue(bucket.realized_pnl || 0)}">${fmtUsd(bucket.realized_pnl || 0)}</strong></span>
        </div>
      </div>`);
    });
    fillBySide.slice(0, 2).forEach((bucket) => {
      breakdownCards.push(`<div class="component-card">
        <span>方向 ${String(bucket.side || "UNKNOWN")}</span>
        <b>${Number(bucket.fill_count || 0)} fills</b>
        <div class="tiny-list">
          <span><i>金额</i><strong>${fmtUsd(bucket.notional || 0, false)}</strong></span>
          <span><i>已实现</i><strong class="${clsForValue(bucket.realized_pnl || 0)}">${fmtUsd(bucket.realized_pnl || 0)}</strong></span>
        </div>
      </div>`);
    });
    breakdownMetaEl.textContent = `${fillBySource.length} sources / ${fillBySide.length} sides`;
    breakdownEl.innerHTML = breakdownCards.length > 0
      ? breakdownCards.join("")
      : '<div class="component-card"><span>成交分解</span><b>当日暂无 fill</b></div>';
  }

  function renderDiagnostics(data, monitor30m, monitor12h, eodReport, now) {
    const metaEl = $("diagnostics-meta");
    const startupListEl = $("startup-checks-list");
    const factsListEl = $("reconciliation-facts-list");
    const issuesListEl = $("diagnostics-issues-list");
    const pendingMetaEl = $("diagnostic-pending-meta");
    const pendingBodyEl = $("diagnostic-pending-body");
    const focusMetaEl = $("diagnostic-focus-meta");
    const focusHeadEl = $("diagnostic-focus-head");
    const focusSummaryEl = $("diagnostic-focus-summary");
    const focusListEl = $("diagnostic-focus-list");
    if (!metaEl || !startupListEl || !factsListEl || !issuesListEl || !pendingMetaEl || !pendingBodyEl || !focusMetaEl || !focusHeadEl || !focusSummaryEl || !focusListEl) return;

    const state = data && typeof data === "object" ? data : {};
    const startup = state.startup && typeof state.startup === "object" ? state.startup : {};
    const reconciliation = state.reconciliation && typeof state.reconciliation === "object" ? state.reconciliation : {};
    const control = state.control && typeof state.control === "object" ? state.control : {};
    const operatorFeedback = state.operator_feedback && typeof state.operator_feedback === "object" ? state.operator_feedback : {};
    const lastOperatorAction = operatorFeedback.last_action && typeof operatorFeedback.last_action === "object" ? operatorFeedback.last_action : {};
    const report30 = monitor30m && typeof monitor30m === "object" ? monitor30m : EMPTY_MONITOR_REPORT("monitor_30m");
    const report12 = monitor12h && typeof monitor12h === "object" ? monitor12h : EMPTY_MONITOR_REPORT("monitor_12h");
    const eod = eodReport && typeof eodReport === "object" ? eodReport : EMPTY_RECONCILIATION_EOD_REPORT;
    const eodRecommendations = Array.isArray(eod.recommendations) ? eod.recommendations : [];
    const eodIssues = Array.isArray(eod.issues) ? eod.issues : [];
    const startupChecks = Array.isArray(startup.checks) ? startup.checks : [];
    const pendingOrders = Array.isArray(state.pending_order_details)
      ? state.pending_order_details.map((row, index) => ({ ...row, _diagKey: pendingOrderRowKey(row, index) }))
      : [];
    const operatorRequestTs = Number(control.clear_stale_pending_requested_ts || 0);
    const operatorActionProcessedTs = Number(lastOperatorAction.processed_ts || 0);
    const operatorActionStatus = operatorRequestTs > operatorActionProcessedTs
      ? { ...lastOperatorAction, status: "requested" }
      : lastOperatorAction;
    const operatorMeta = operatorActionMeta(operatorActionStatus);
    const generatedTs = Math.max(Number(report30.generated_ts || 0), Number(report12.generated_ts || 0), Number(eod.generated_ts || 0), Number(state.ts || 0));

    lastDiagnosticsState = state;
    lastDiagnosticsMonitor30 = report30;
    lastDiagnosticsMonitor12 = report12;
    lastDiagnosticsEod = eod;
    lastDiagnosticsNow = now;

    metaEl.textContent = generatedTs > 0
      ? `${fmtDateTime(generatedTs)} · ${fmtAge(Math.max(0, Math.floor(Date.now() / 1000) - generatedTs))}前`
      : "startup / reconciliation / monitor";

    const startupKeys = new Set(startupChecks.map((row) => String(row && row.name || "startup")));
    const pendingKeys = new Set(pendingOrders.map((row) => String(row._diagKey || "")));
    const selectedStartupExists = selectedDiagnosticFocusKind === "startup" && startupKeys.has(selectedDiagnosticFocusKey);
    const selectedPendingExists = selectedDiagnosticFocusKind === "pending" && pendingKeys.has(selectedDiagnosticFocusKey);
    if (!selectedStartupExists && !selectedPendingExists) {
      const nextFocus = selectDefaultDiagnosticFocus(startupChecks, pendingOrders);
      selectedDiagnosticFocusKind = nextFocus.kind;
      selectedDiagnosticFocusKey = nextFocus.key;
    }

    startupListEl.innerHTML = startupChecks.length > 0
      ? startupChecks.slice(0, 8).map((row) => {
          const [cls, tag] = startupCheckMeta(row && row.status);
          const details = row && row.details && typeof row.details === "object" ? row.details : null;
          const detailText = details
            ? Object.entries(details)
                .slice(0, 3)
                .map(([key, value]) => `${key}=${String(value)}`)
                .join(" · ")
            : "";
          const rowKey = String(row && row.name || "startup");
          const active = selectedDiagnosticFocusKind === "startup" && selectedDiagnosticFocusKey === rowKey;
          return `<li class="review-selectable${active ? " active" : ""}" data-diagnostic-kind="startup" data-diagnostic-key="${attrToken(rowKey)}">
            <div class="review-main">
              <span>${String(row && row.name || "startup")}</span>
              <b><span class="tag ${cls}">${tag}</span></b>
            </div>
            <div class="review-sub">${String(row && row.message || "no message")}${detailText ? ` · ${detailText}` : ""}</div>
          </li>`;
        }).join("")
      : '<li><div class="review-main"><span>暂无 startup checks</span><b>--</b></div></li>';

    const facts = [
      {
        label: "internal vs ledger",
        value: fmtUsd(reconciliation.internal_vs_ledger_diff || 0),
        sub: `broker floor gap ${fmtUsd(reconciliation.broker_floor_gap_vs_internal || 0)}`,
        cls: clsForValue(reconciliation.internal_vs_ledger_diff || 0),
      },
      {
        label: "pending / stale",
        value: `${Number(reconciliation.pending_orders || 0)} / ${Number(reconciliation.stale_pending_orders || 0)}`,
        sub: `entry ${Number(reconciliation.pending_entry_orders || 0)} · exit ${Number(reconciliation.pending_exit_orders || 0)}`,
        cls: Number(reconciliation.stale_pending_orders || 0) > 0 ? "value-negative" : "value-neutral",
      },
      {
        label: "snapshot age",
        value: fmtAge(reconciliation.account_snapshot_age_seconds || 0),
        sub: `reconcile ${fmtAge(reconciliation.broker_reconcile_age_seconds || 0)} · events ${fmtAge(reconciliation.broker_event_sync_age_seconds || 0)}`,
        cls: Number(reconciliation.account_snapshot_age_seconds || 0) > 1800 ? "value-negative" : "value-neutral",
      },
      {
        label: "fills today",
        value: `${Number(reconciliation.fill_count_today || 0)}`,
        sub: `notional ${fmtUsd(reconciliation.fill_notional_today || 0, false)} · account sync ${Number(reconciliation.account_sync_count_today || 0)}`,
        cls: "value-neutral",
      },
      {
        label: "startup checks today",
        value: `${Number(reconciliation.startup_checks_count_today || 0)}`,
        sub: `last fill ${Number(reconciliation.last_fill_ts || 0) > 0 ? historyAgeLabel(reconciliation.last_fill_ts, now) : "未记录"}`,
        cls: "value-neutral",
      },
      {
        label: "open / tracked",
        value: `${Number(reconciliation.open_positions || 0)} / ${fmtUsd(reconciliation.tracked_notional_usd || 0, false)}`,
        sub: `status ${String(reconciliation.status || "unknown")}`,
        cls: "value-neutral",
      },
      {
        label: "operator cleanup",
        value: operatorMeta.label,
        sub: operatorRequestTs > 0
          ? `${operatorActionProcessedTs > 0 ? `processed ${fmtDateTime(operatorActionProcessedTs)}` : "request queued"} · request ${fmtDateTime(operatorRequestTs)}${operatorActionStatus.remaining_pending_orders != null ? ` · remain ${Number(operatorActionStatus.remaining_pending_orders || 0)}` : ""}`
          : "尚未请求 clear_stale_pending",
        cls: operatorMeta.valueCls,
      },
    ];
    factsListEl.innerHTML = facts.map((item) => `<li>
      <div class="review-main">
        <span>${item.label}</span>
        <b class="${item.cls}">${item.value}</b>
      </div>
      <div class="review-sub">${item.sub}</div>
    </li>`).join("");

    const issueRows = [];
    const pushIssue = (title, detail, status = "wait") => {
      const safeTitle = String(title || "").trim();
      const safeDetail = String(detail || "").trim();
      if (!safeTitle || !safeDetail) return;
      issueRows.push({ title: safeTitle, detail: safeDetail, status });
    };

    const stateIssues = Array.isArray(reconciliation.issues) ? reconciliation.issues : [];
    stateIssues.slice(0, 4).forEach((item) => pushIssue("state reconciliation", String(item), String(reconciliation.status || "wait")));
    if (report30.final_recommendation) pushIssue("30m monitor", String(report30.final_recommendation), recommendationKind(report30.final_recommendation) === "ready" ? "ok" : recommendationKind(report30.final_recommendation));
    if (report12.final_recommendation) pushIssue("12h monitor", String(report12.final_recommendation), recommendationKind(report12.final_recommendation) === "ready" ? "ok" : recommendationKind(report12.final_recommendation));
    eodIssues.slice(0, 3).forEach((item) => pushIssue("EOD issue", String(item), String(eod.status || "wait")));
    eodRecommendations.slice(0, 3).forEach((item) => pushIssue("EOD recommendation", String(item), String(eod.status || "wait")));
    if (operatorRequestTs > 0) {
      if (operatorRequestTs > operatorActionProcessedTs) {
        pushIssue("operator action", "clear_stale_pending 已发出请求，等待下一轮 runner 周期消费。", "wait");
      } else if (String(operatorActionStatus.message || "").trim()) {
        const operatorIssueStatus = String(operatorActionStatus.status || "wait");
        pushIssue(
          "operator action",
          String(operatorActionStatus.message || ""),
          operatorIssueStatus === "cleared" || operatorIssueStatus === "noop" ? "ok" : "wait"
        );
      }
    }
    if (issueRows.length === 0) {
      pushIssue("current diagnosis", "当前 startup、自检、monitor 和 reconciliation 没有突出异常，继续观察下一轮报告。", "ok");
    }

    issuesListEl.innerHTML = issueRows.slice(0, 8).map((item) => {
      const [cls, tag] = reportStatusMeta(item.status);
      return `<li>
        <div class="review-main">
          <span>${item.title}</span>
          <b><span class="tag ${cls}">${tag}</span></b>
        </div>
        <div class="review-sub">${item.detail}</div>
      </li>`;
    }).join("");

    const stalePending = Number(reconciliation.stale_pending_orders || 0);
    pendingMetaEl.textContent = `${pendingOrders.length} orders${stalePending > 0 ? ` · stale ${stalePending}` : ""}`;
    replaceRows(
      pendingBodyEl,
      pendingOrders.slice(0, 12).map((row) => {
        const [statusCls, statusTag] = pendingOrderStatusMeta(row.broker_status || "live");
        const [flowCls, flowTag] = actionTagMeta(row.flow === "exit" ? "exit" : "entry", row.flow === "exit" ? "退出" : "入场");
        const requestedText = row.requested_notional > 0
          ? `${fmtUsd(row.requested_notional || 0, false)} @ ${Number(row.requested_price || 0).toFixed(4)}`
          : "--";
        const matchedText = row.matched_notional_hint > 0
          ? `${fmtUsd(row.matched_notional_hint || 0, false)} @ ${Number(row.matched_price_hint || 0).toFixed(4)}`
          : "--";
        const reasonText = String(row.reason || row.message || "--");
        const rowClass = selectedDiagnosticFocusKind === "pending" && selectedDiagnosticFocusKey === row._diagKey
          ? "click-row active-row"
          : "click-row";
        return `<tr class="${rowClass}" data-diagnostic-kind="pending" data-diagnostic-key="${attrToken(row._diagKey)}">
          <td class="wrap">
            <div class="cell-stack">
              <span class="cell-main">${Number(row.ts || 0) > 0 ? hhmm(row.ts) : "--:--"}</span>
              <span class="cell-sub">${Number(row.ts || 0) > 0 ? historyAgeLabel(row.ts, now) : "未记录"}</span>
            </div>
          </td>
          <td class="wrap">
            <div class="cell-stack">
              <span class="cell-main">${String(row.title || row.market_slug || row.token_id || "-")}</span>
              <span class="cell-sub">${String(row.outcome || row.side || "-")}${row.condition_id ? ` · ${row.condition_id}` : ""}</span>
            </div>
          </td>
          <td class="wrap">
            <div class="cell-stack">
              <span><span class="tag ${flowCls}">${flowTag}</span> <span class="tag ${statusCls}">${statusTag}</span></span>
              <span class="cell-sub">${String(row.broker_status || "pending")}${row.order_id ? ` · ${row.order_id}` : ""}</span>
            </div>
          </td>
          <td class="wrap">
            <div class="cell-stack">
              <span class="cell-main">${requestedText}</span>
              <span class="cell-sub">${matchedText}</span>
            </div>
          </td>
          <td class="wrap">${reasonText}</td>
        </tr>`;
      }),
      '<tr><td colspan="5">当前没有活跃 pending / stale 订单</td></tr>'
    );

    const selectedStartup = selectedDiagnosticFocusKind === "startup"
      ? startupChecks.find((row) => String(row && row.name || "startup") === selectedDiagnosticFocusKey) || null
      : null;
    const selectedPending = selectedDiagnosticFocusKind === "pending"
      ? pendingOrders.find((row) => String(row._diagKey || "") === selectedDiagnosticFocusKey) || null
      : null;

    if (selectedStartup) {
      const [cls, tag] = startupCheckMeta(selectedStartup.status);
      const details = selectedStartup.details && typeof selectedStartup.details === "object" ? selectedStartup.details : {};
      const rows = [
        `<li><span>检查项</span><b>${String(selectedStartup.name || "startup")}</b></li>`,
        `<li><span>状态</span><b>${tag}</b></li>`,
        `<li><span>提示</span><b>${String(selectedStartup.message || "暂无说明")}</b></li>`,
      ];
      Object.entries(details).forEach(([key, value]) => {
        rows.push(`<li><span>${String(key)}</span><b>${String(value)}</b></li>`);
      });
      focusMetaEl.textContent = `startup · ${String(selectedStartup.name || "startup")}`;
      focusHeadEl.innerHTML = `<span class="tag ${cls}">${tag}</span><span class="mono">${String(selectedStartup.name || "startup")}</span>`;
      focusSummaryEl.textContent = String(selectedStartup.message || "当前启动检查没有额外说明");
      focusListEl.innerHTML = rows.join("");
      return;
    }

    if (selectedPending) {
      const [statusCls, statusTag] = pendingOrderStatusMeta(selectedPending.broker_status || "live");
      const [flowCls, flowTag] = actionTagMeta(selectedPending.flow === "exit" ? "exit" : "entry", selectedPending.flow === "exit" ? "退出 pending" : "入场 pending");
      const focusRows = [
        `<li><span>市场 / 方向</span><b>${String(selectedPending.title || selectedPending.market_slug || selectedPending.token_id || "-")} · ${String(selectedPending.outcome || selectedPending.side || "-")}</b></li>`,
        `<li><span>订单 / Trace</span><b>${String(selectedPending.order_id || "--")} · ${String(selectedPending.trace_id || "--")}</b></li>`,
        `<li><span>请求金额</span><b>${fmtUsd(selectedPending.requested_notional || 0, false)} @ ${Number(selectedPending.requested_price || 0).toFixed(4)}</b></li>`,
        `<li><span>已匹配提示</span><b>${selectedPending.matched_notional_hint > 0 ? `${fmtUsd(selectedPending.matched_notional_hint || 0, false)} @ ${Number(selectedPending.matched_price_hint || 0).toFixed(4)}` : "暂无 matched hint"}</b></li>`,
        `<li><span>来源钱包</span><b>${selectedPending.wallet ? `${sourceWalletLabel(selectedPending.wallet)}${selectedPending.wallet_tier ? ` · ${selectedPending.wallet_tier}` : ""}${Number(selectedPending.wallet_score || 0) > 0 ? ` · ${Number(selectedPending.wallet_score || 0).toFixed(1)}` : ""}` : "未标记来源"}</b></li>`,
        `<li><span>入场钱包</span><b>${selectedPending.entry_wallet ? `${sourceWalletLabel(selectedPending.entry_wallet)}${selectedPending.entry_wallet_tier ? ` · ${selectedPending.entry_wallet_tier}` : ""}${Number(selectedPending.entry_wallet_score || 0) > 0 ? ` · ${Number(selectedPending.entry_wallet_score || 0).toFixed(1)}` : ""}` : "未标记 entry wallet"}</b></li>`,
        `<li><span>Cycle / Signal</span><b>${String(selectedPending.cycle_id || "--")} · ${String(selectedPending.signal_id || "--")}</b></li>`,
        `<li><span>时间 / 心跳</span><b>${Number(selectedPending.ts || 0) > 0 ? `${fmtDateTime(selectedPending.ts)} · ${historyAgeLabel(selectedPending.ts, now)}` : "未记录"}${Number(selectedPending.last_heartbeat_ts || 0) > 0 ? ` / heartbeat ${historyAgeLabel(selectedPending.last_heartbeat_ts, now)}` : ""}</b></li>`,
      ];
      if (selectedPending.topic_label) {
        focusRows.push(`<li><span>题材</span><b>${String(selectedPending.topic_label)}</b></li>`);
      }
      if (selectedPending.condition_id || selectedPending.token_id) {
        focusRows.push(`<li><span>Condition / Token</span><b>${String(selectedPending.condition_id || "--")} · ${String(selectedPending.token_id || "--")}</b></li>`);
      }
      focusMetaEl.textContent = `pending · ${String(selectedPending.order_id || selectedPending.title || "order")}`;
      focusHeadEl.innerHTML =
        `<span class="tag ${flowCls}">${flowTag}</span>` +
        `<span class="tag ${statusCls}">${statusTag}</span>` +
        `<span class="mono">${String(selectedPending.broker_status || "pending")}</span>`;
      focusSummaryEl.textContent = String(selectedPending.reason || selectedPending.message || "当前 pending 订单没有额外说明");
      focusListEl.innerHTML = focusRows.join("");
      return;
    }

    focusMetaEl.textContent = "未选择";
    focusHeadEl.innerHTML = '<span class="tag danger">等待</span><span class="mono">点击 startup check 或 pending 订单查看详情</span>';
    focusSummaryEl.textContent = "这里会展示启动自检或 pending 订单的结构化细节，帮助 operator 快速判断下一步。";
    focusListEl.innerHTML = "<li><span>状态</span><b>等待数据...</b></li>";
  }

  function bindExitReviewFilters() {
    const bindList = (id) => {
      const el = $(id);
      if (!el || el.dataset.bound === "1") return;
      el.dataset.bound = "1";
      el.addEventListener("click", (event) => {
        const item = event.target.closest("li[data-filter-type][data-filter-value]");
        if (!item) return;
        const filterType = String(item.getAttribute("data-filter-type") || "");
        const filterValue = readAttrToken(item.getAttribute("data-filter-value") || "");
        if (!filterType || !Object.prototype.hasOwnProperty.call(exitReviewFilter, filterType)) return;
        exitReviewFilter[filterType] = exitReviewFilter[filterType] === filterValue ? "" : filterValue;
        persistExitReviewUiState();
        refresh();
      });
    };
    bindList("exit-review-kind");
    bindList("exit-review-topic");
    bindList("exit-review-source");

    const resetBtn = $("exit-review-filter-reset");
    if (resetBtn && resetBtn.dataset.bound !== "1") {
      resetBtn.dataset.bound = "1";
      resetBtn.addEventListener("click", () => {
        exitReviewFilter.kind = "";
        exitReviewFilter.topic = "";
        exitReviewFilter.source = "";
        persistExitReviewUiState();
        refresh();
      });
    }
  }

  function bindExitReviewSamples() {
    const tbody = $("exit-review-samples-body");
    if (!tbody || tbody.dataset.bound === "1") return;
    tbody.dataset.bound = "1";
    tbody.addEventListener("click", (event) => {
      const row = event.target.closest("tr[data-exit-sample-key]");
      if (!row) return;
      selectedExitSampleKey = readAttrToken(row.getAttribute("data-exit-sample-key") || "");
      const traceId = readAttrToken(row.getAttribute("data-trace-id") || "");
      if (traceId) {
        selectedTraceId = traceId;
        selectedSignalCycleId = latestCycleIdForTrace(lastSignalReview || {}, traceId) || selectedSignalCycleId;
      }
      persistExitReviewUiState();
      renderExitReview(lastExitReview || {}, lastExitReviewNow || 0);
      renderSignalReview(lastSignalReview || {}, lastSignalReviewNow || 0);
    });
  }

  function bindTraceReviewInteractions() {
    const positionsBody = $("positions-body");
    if (positionsBody && positionsBody.dataset.traceBound !== "1") {
      positionsBody.dataset.traceBound = "1";
      positionsBody.addEventListener("click", (event) => {
        const row = event.target.closest("tr[data-trace-id]");
        if (!row) return;
        const traceId = readAttrToken(row.getAttribute("data-trace-id") || "");
        if (!traceId) return;
        selectedTraceId = traceId;
        selectedSignalCycleId = latestCycleIdForTrace(lastSignalReview || {}, traceId) || selectedSignalCycleId;
        persistExitReviewUiState();
        renderSignalReview(lastSignalReview || {}, lastSignalReviewNow || 0);
      });
    }

    const cyclesList = $("trace-review-cycles");
    if (cyclesList && cyclesList.dataset.bound !== "1") {
      cyclesList.dataset.bound = "1";
      cyclesList.addEventListener("click", (event) => {
        const item = event.target.closest("li[data-signal-cycle-id]");
        if (!item) return;
        selectedSignalCycleId = readAttrToken(item.getAttribute("data-signal-cycle-id") || "");
        persistExitReviewUiState();
        renderSignalReview(lastSignalReview || {}, lastSignalReviewNow || 0);
      });
    }

    const cycleBody = $("trace-review-cycle-body");
    if (cycleBody && cycleBody.dataset.bound !== "1") {
      cycleBody.dataset.bound = "1";
      cycleBody.addEventListener("click", (event) => {
        const row = event.target.closest("tr[data-trace-id]");
        if (!row) return;
        const traceId = readAttrToken(row.getAttribute("data-trace-id") || "");
        const cycleId = readAttrToken(row.getAttribute("data-signal-cycle-id") || "");
        if (traceId) {
          selectedTraceId = traceId;
        }
        if (cycleId) {
          selectedSignalCycleId = cycleId;
        }
        persistExitReviewUiState();
        renderSignalReview(lastSignalReview || {}, lastSignalReviewNow || 0);
      });
    }
  }

  function bindAttributionWindows() {
    const chips = $("attribution-window-chips");
    if (!chips || chips.dataset.bound === "1") return;
    chips.dataset.bound = "1";
    chips.addEventListener("click", (event) => {
      const chip = event.target.closest("[data-window]");
      if (!chip) return;
      selectedAttributionWindow = String(chip.getAttribute("data-window") || "24h");
      persistExitReviewUiState();
      renderAttributionReview(lastAttributionReview || {}, lastAttributionReviewNow || 0);
    });
  }

  function bindOpsGateActions() {
    const actionsEl = $("ops-gate-actions");
    if (!actionsEl || actionsEl.dataset.bound === "1") return;
    actionsEl.dataset.bound = "1";
    actionsEl.addEventListener("click", async (event) => {
      const button = event.target.closest("button[data-ops-action][data-ops-value]");
      if (!button) return;
      const action = String(button.getAttribute("data-ops-action") || "");
      const value = readAttrToken(button.getAttribute("data-ops-value") || "");
      if (!action || !value) return;

      const original = button.textContent || "";
      if (action === "copy") {
        const ok = await copyText(value);
        button.textContent = ok ? "已复制" : "复制失败";
        window.setTimeout(() => {
          button.textContent = original;
        }, 1200);
        return;
      }
      if (action === "jump") {
        const target = document.getElementById(value);
        if (target && typeof target.scrollIntoView === "function") {
          target.scrollIntoView({ behavior: "smooth", block: "start" });
        }
        return;
      }
      if (action === "open") {
        window.open(value, "_blank", "noopener,noreferrer");
        return;
      }
      if (action === "api") {
        button.disabled = true;
        try {
          await runOperator(value);
          button.textContent = "已刷新";
          await refresh();
        } catch (_err) {
          button.textContent = "刷新失败";
        } finally {
          window.setTimeout(() => {
            button.disabled = false;
            button.textContent = original;
          }, 1200);
        }
      }
    });
  }

  function bindDiagnosticsInteractions() {
    const startupList = $("startup-checks-list");
    if (startupList && startupList.dataset.bound !== "1") {
      startupList.dataset.bound = "1";
      startupList.addEventListener("click", (event) => {
        const item = event.target.closest("li[data-diagnostic-kind][data-diagnostic-key]");
        if (!item) return;
        selectedDiagnosticFocusKind = String(item.getAttribute("data-diagnostic-kind") || "");
        selectedDiagnosticFocusKey = readAttrToken(item.getAttribute("data-diagnostic-key") || "");
        renderDiagnostics(lastDiagnosticsState || {}, lastDiagnosticsMonitor30 || {}, lastDiagnosticsMonitor12 || {}, lastDiagnosticsEod || {}, lastDiagnosticsNow || 0);
      });
    }

    const pendingBody = $("diagnostic-pending-body");
    if (pendingBody && pendingBody.dataset.bound !== "1") {
      pendingBody.dataset.bound = "1";
      pendingBody.addEventListener("click", (event) => {
        const row = event.target.closest("tr[data-diagnostic-kind][data-diagnostic-key]");
        if (!row) return;
        selectedDiagnosticFocusKind = String(row.getAttribute("data-diagnostic-kind") || "");
        selectedDiagnosticFocusKey = readAttrToken(row.getAttribute("data-diagnostic-key") || "");
        renderDiagnostics(lastDiagnosticsState || {}, lastDiagnosticsMonitor30 || {}, lastDiagnosticsMonitor12 || {}, lastDiagnosticsEod || {}, lastDiagnosticsNow || 0);
      });
    }
  }

  function bindWalletSelection() {
    for (const id of ["wallets-body", "sources-body"]) {
      const tbody = $(id);
      if (!tbody || tbody.dataset.bound === "1") continue;
      tbody.dataset.bound = "1";
      tbody.addEventListener("click", (event) => {
        const row = event.target.closest("tr[data-wallet]");
        if (!row) return;
        const wallet = normalizeWallet(row.getAttribute("data-wallet"));
        if (!wallet) return;
        selectedWallet = wallet;
        refresh();
      });
    }
  }

  function renderWalletDetail(wallet, config, now) {
    const titleEl = $("wallet-detail-title");
    const headEl = $("wallet-detail-head");
    const summaryEl = $("wallet-detail-summary");
    const listEl = $("wallet-detail-list");
    const componentEl = $("wallet-component-grid");
    const topicMetaEl = $("wallet-topic-meta");
    const topicGridEl = $("wallet-topic-grid");
    const historyMetaEl = $("wallet-detail-history-meta");
    const historyBodyEl = $("wallet-detail-history-body");
    if (!titleEl || !headEl || !summaryEl || !listEl || !componentEl || !topicMetaEl || !topicGridEl || !historyMetaEl || !historyBodyEl) return;

    if (!wallet) {
      titleEl.textContent = "未选择";
      headEl.innerHTML = '<span class="tag danger">等待</span><span class="mono">点击钱包行查看详情</span>';
      summaryEl.textContent = "这里会展示单个钱包的评分逻辑、历史样本和真实表现。";
      listEl.innerHTML = "<li><span>状态</span><b>等待数据...</b></li>";
      componentEl.innerHTML = '<div class="component-card"><span>评分拆解</span><b>等待数据...</b></div>';
      topicMetaEl.textContent = "0 themes";
      topicGridEl.innerHTML = '<div class="topic-card"><span>题材偏好</span><b>等待数据...</b></div>';
      historyMetaEl.textContent = "0 samples";
      historyBodyEl.innerHTML = '<tr><td colspan="5">等待数据...</td></tr>';
      return;
    }

    const tier = String(wallet.tier || "LOW").toUpperCase();
    const history = historyProfile(wallet, config);
    const tradingEnabled = !!wallet.trading_enabled;
    const realized = wallet.realized_metrics || {};
    const scoreComponents = wallet.score_components || {};
    const wins = Number(realized.wins || 0);
    const resolvedWins = Number(realized.resolved_wins || 0);
    const totalBought = Number(realized.total_bought || 0);
    const realizedPnl = Number(realized.realized_pnl || 0);
    const scoreSummary = String(wallet.score_summary || "").trim();
    const topicProfiles = Array.isArray(wallet.topic_profiles) ? wallet.topic_profiles.slice(0, 4) : [];
    const recentClosedMarkets = Array.isArray(wallet.recent_closed_markets) ? wallet.recent_closed_markets.slice(0, 5) : [];

    titleEl.textContent = shortWallet(wallet.wallet);
    headEl.innerHTML =
      `<span class="tag ${tierTagClass(tier)}">${tier}</span>` +
      `<span class="tag ${history.cls}">${history.label}</span>` +
      `<span class="mono">${tradingEnabled ? "trade enabled" : "observe only"}</span>`;
    summaryEl.textContent = scoreSummary || "暂无评分摘要";

    listEl.innerHTML = [
      `<li><span>当前状态</span><b>${Number(wallet.score || 0).toFixed(1)} 分 · ${tradingEnabled ? "允许交易" : "仅观察"}</b></li>`,
      `<li><span>当前仓位画像</span><b>${Number(wallet.positions || 0)} pos · ${Number(wallet.unique_markets || 0)} mkts · ${Number(wallet.notional || 0).toFixed(0)}U</b></li>`,
      `<li><span>历史样本</span><b>${Number(wallet.closed_positions || 0)} closed / ${Number(wallet.resolved_markets || 0)} resolved · ${historyAgeLabel(wallet.history_refresh_ts, now)}</b></li>`,
      `<li><span>真实表现</span><b class="${clsForValue(wallet.roi)}">ROI ${fmtSignedRatioPct(wallet.roi, 1)} · Win ${fmtRatioPct(wallet.win_rate, 1)}</b></li>`,
      `<li><span>解析命中 / 盈亏比</span><b>${Number(wallet.resolved_markets || 0) > 0 ? fmtRatioPct(wallet.resolved_win_rate, 1) : "--"} · PF ${Number(wallet.profit_factor || 0).toFixed(2)}</b></li>`,
      `<li><span>历史盈亏</span><b class="${clsForValue(realizedPnl)}">${fmtUsd(realizedPnl)} / 买入 ${fmtUsd(totalBought, false)}</b></li>`,
      `<li><span>近期活跃</span><b>${wallet.activity_known ? `${Number(wallet.recent_activity_events || 0)} events` : "unknown"}</b></li>`,
      `<li><span>钱包池优先级</span><b>#${Number(wallet.discovery_priority_rank || 0)} · ${Number(wallet.discovery_priority_score || 0).toFixed(2)}${wallet.discovery_best_topic ? ` · ${wallet.discovery_best_topic}` : ""}</b></li>`,
      `<li><span>胜场拆解</span><b>${wins}/${Number(wallet.closed_positions || 0)} · resolved ${resolvedWins}/${Number(wallet.resolved_markets || 0)}</b></li>`,
    ].join("");

    const componentRows = Object.entries(scoreComponents);
    componentEl.innerHTML = componentRows.length > 0
      ? componentRows
          .map(([key, value]) => {
            const numeric = Number(value || 0);
            return `<div class="component-card">
              <span>${componentLabel(key)}</span>
              <b class="${numeric >= 12 ? "value-positive" : numeric >= 6 ? "warn" : "value-neutral"}">${numeric.toFixed(1)}</b>
            </div>`;
          })
          .join("")
      : '<div class="component-card"><span>评分拆解</span><b>暂无数据</b></div>';

    topicMetaEl.textContent = `${topicProfiles.length} themes`;
    topicGridEl.innerHTML = topicProfiles.length > 0
      ? topicProfiles
          .map((topic) => {
            const resolvedText = Number(topic.resolved_markets || 0) > 0
              ? `resolved ${fmtRatioPct(topic.resolved_win_rate, 0)}`
              : "resolved --";
            return `<div class="topic-card">
              <span>${String(topic.label || topic.key || "其他")} · ${(Number(topic.sample_share || 0) * 100).toFixed(0)}%</span>
              <b class="${topicTone(topic)}">${fmtSignedRatioPct(topic.roi, 1)} / ${fmtRatioPct(topic.win_rate, 0)}</b>
              <span>${Number(topic.sample_count || 0)} samples · ${resolvedText}</span>
            </div>`;
          })
          .join("")
      : '<div class="topic-card"><span>题材偏好</span><b>暂无数据</b></div>';

    historyMetaEl.textContent = `${recentClosedMarkets.length} samples`;
    historyBodyEl.innerHTML = recentClosedMarkets.length > 0
      ? recentClosedMarkets
          .map((sample) => {
            const [verdictCls, verdictText] = sampleVerdict(sample);
            const winnerText = sample.winner_outcome ? ` / win ${sample.winner_outcome}` : "";
            return `<tr>
              <td class="wrap">
                <div class="cell-stack">
                  <span class="cell-main">${String(sample.market_slug || "-")}</span>
                  <span class="cell-sub">${sample.end_date || historyAgeLabel(sample.timestamp, now)}</span>
                </div>
              </td>
              <td>${String(sample.outcome || "-")}</td>
              <td class="wrap">
                <div class="cell-stack">
                  <span><span class="tag ${verdictCls}">${verdictText}</span></span>
                  <span class="cell-sub">${sample.resolved ? `resolved${winnerText}` : "unresolved"}</span>
                </div>
              </td>
              <td class="${clsForValue(sample.roi)}">${fmtSignedRatioPct(sample.roi, 1)}</td>
              <td class="${clsForValue(sample.realized_pnl)}">${fmtUsd(sample.realized_pnl)}</td>
            </tr>`;
          })
          .join("")
      : '<tr><td colspan="5">暂无已结算样本</td></tr>';
  }

  function updateModeBadge(config) {
    const pill = $("mode-pill");
    if (!pill) return;
    const mode = String(config.execution_mode || (config.dry_run ? "paper" : "live")).toLowerCase();
    const label = mode === "live" ? "LIVE" : "PAPER";
    pill.textContent = label;
    pill.classList.toggle("live", mode === "live");
    pill.classList.toggle("paper", mode !== "live");
  }

  function updateStrategyParams(config) {
    if ($("param-wallet-pool")) $("param-wallet-pool").textContent = `wallet_pool=${Number(config.wallet_pool_size || 0)}`;
    if ($("param-min-increase")) $("param-min-increase").textContent = `${Number(config.min_wallet_increase_usd || 0).toFixed(0)} USD`;
    if ($("param-max-signals")) $("param-max-signals").textContent = String(config.max_signals_per_cycle || 0);
    if ($("param-min-wallet-score")) $("param-min-wallet-score").textContent = Number(config.min_wallet_score || 0).toFixed(1);
    if ($("param-score-multipliers")) {
      $("param-score-multipliers").textContent =
        `W ${Number(config.wallet_score_watch_multiplier || 0).toFixed(2)} / ` +
        `T ${Number(config.wallet_score_trade_multiplier || 0).toFixed(2)} / ` +
        `C ${Number(config.wallet_score_core_multiplier || 0).toFixed(2)}`;
    }
    if ($("param-history-window")) {
      $("param-history-window").textContent =
        `${fmtAge(Number(config.wallet_history_refresh_seconds || 0))} / ` +
        `min ${Number(config.history_min_closed_positions || 0)} / ` +
        `strong ${Number(config.history_strong_closed_positions || 0)}c ${Number(config.history_strong_resolved_markets || 0)}r`;
    }
    if ($("param-topic-bias")) {
      if (!config.topic_bias_enabled) {
        $("param-topic-bias").textContent = "off";
      } else {
        $("param-topic-bias").textContent =
          `min ${Number(config.topic_min_samples || 0)} / ` +
          `+${Number(config.topic_boost_multiplier || 0).toFixed(2)} / ` +
          `-${Number(config.topic_penalty_multiplier || 0).toFixed(2)}`;
      }
    }
    if ($("param-discovery-bias")) {
      if (!config.wallet_discovery_quality_bias_enabled) {
        $("param-discovery-bias").textContent = "off";
      } else {
        $("param-discovery-bias").textContent =
          `top ${Number(config.wallet_discovery_quality_top_n || 0)} / ` +
          `hist +${Number(config.wallet_discovery_history_bonus || 0).toFixed(2)} / ` +
          `topic +${Number(config.wallet_discovery_topic_bonus || 0).toFixed(2)}`;
      }
    }
    if ($("param-wallet-exit")) {
      if (!config.wallet_exit_follow_enabled) {
        $("param-wallet-exit").textContent = "off";
      } else if (!config.resonance_exit_enabled) {
        $("param-wallet-exit").textContent = `single / min ${Number(config.min_wallet_decrease_usd || 0).toFixed(0)} USD`;
      } else {
        $("param-wallet-exit").textContent =
          `single ${Number(config.min_wallet_decrease_usd || 0).toFixed(0)} / ` +
          `res ${Number(config.resonance_min_wallets || 0)} wallets / ` +
          `${fmtPct(Number(config.resonance_trim_fraction || 0) * 100, 0)} -> ${fmtPct(Number(config.resonance_core_exit_fraction || 0) * 100, 0)}`;
      }
    }
    if ($("param-risk-trade")) $("param-risk-trade").textContent = fmtPct(Number(config.risk_per_trade_pct || 0) * 100, 2);
    if ($("param-risk-day")) $("param-risk-day").textContent = fmtPct(Number(config.daily_max_loss_pct || 0) * 100, 2);
    if ($("param-price-band")) $("param-price-band").textContent = `${Number(config.min_price || 0).toFixed(2)} ~ ${Number(config.max_price || 0).toFixed(2)}`;
    if ($("param-add-cooldown")) $("param-add-cooldown").textContent = `${Number(config.token_add_cooldown_seconds || 0)}s`;
  }

  function updateOrderChips(orders) {
    let pending = 0;
    let filled = 0;
    let canceled = 0;
    let rejected = 0;
    for (const o of orders) {
      const st = String(o.status || "").toUpperCase();
      if (st === "PENDING") pending += 1;
      else if (st === "FILLED") filled += 1;
      else if (st === "CANCELED") canceled += 1;
      else if (st === "REJECTED") rejected += 1;
    }
    if ($("chip-pending")) $("chip-pending").textContent = `进行中 ${pending}`;
    if ($("chip-filled")) $("chip-filled").textContent = `已成交 ${filled}`;
    if ($("chip-canceled")) $("chip-canceled").textContent = `已取消 ${canceled}`;
    if ($("chip-rejected")) $("chip-rejected").textContent = `已拒绝 ${rejected}`;
  }

  function updateRiskMeters(summary, orders, config, stateAgeSec) {
    const used = Number(summary.daily_loss_used_pct || 0);
    const slotUtil = Number(summary.slot_utilization_pct || summary.exposure_pct || 0);
    const maxOpen = Number(summary.max_open_positions || 0);
    const openPos = Number(summary.open_positions || 0);
    const pollSec = Number(config.poll_interval_seconds || 0);

    let cooldownSkips = 0;
    for (const o of orders) {
      if (String(o.reason || "").includes("token add cooldown")) cooldownSkips += 1;
    }
    const cooldownRate = orders.length > 0 ? (cooldownSkips / orders.length) * 100 : 0;
    const freshnessPct = pollSec > 0 ? Math.min(100, (stateAgeSec / pollSec) * 100) : 0;

    if ($("meter-dd")) $("meter-dd").value = Math.max(0, Math.min(100, used));
    if ($("meter-dd-text")) {
      $("meter-dd-text").textContent = `${fmtPct(used, 1)} / 日损上限 ${fmtPct(Number(config.daily_max_loss_pct || 0) * 100, 1)}`;
    }

    if ($("meter-exp")) $("meter-exp").value = Math.max(0, Math.min(100, slotUtil));
    if ($("meter-exp-text")) $("meter-exp-text").textContent = `${openPos} / ${maxOpen}`;

    if ($("meter-cooldown")) $("meter-cooldown").value = Math.max(0, Math.min(100, cooldownRate));
    if ($("meter-cooldown-text")) $("meter-cooldown-text").textContent = `${cooldownSkips} / ${orders.length}`;

    if ($("meter-util")) $("meter-util").value = Math.max(0, Math.min(100, freshnessPct));
    if ($("meter-util-text")) $("meter-util-text").textContent = `${stateAgeSec}s / ${pollSec}s`;
  }

  function setButtonBusy(el, busy) {
    if (!el) return;
    el.disabled = !!busy;
    el.style.opacity = busy ? "0.7" : "1";
  }

  function renderControlState(next) {
    controlState.pause_opening = !!next.pause_opening;
    controlState.reduce_only = !!next.reduce_only;
    controlState.emergency_stop = !!next.emergency_stop;
    controlState.updated_ts = Number(next.updated_ts || 0);

    const pauseBtn = $("btn-pause-opening");
    const reduceBtn = $("btn-reduce-only");
    const emergencyBtn = $("btn-emergency-stop");

    if (pauseBtn) {
      pauseBtn.classList.toggle("active", controlState.pause_opening);
      pauseBtn.textContent = controlState.pause_opening ? "恢复开仓" : "暂停开仓";
    }
    if (reduceBtn) {
      reduceBtn.classList.toggle("active", controlState.reduce_only);
      reduceBtn.textContent = controlState.reduce_only ? "取消只减仓" : "只减仓模式";
    }
    if (emergencyBtn) {
      emergencyBtn.classList.toggle("active", controlState.emergency_stop);
      emergencyBtn.textContent = controlState.emergency_stop ? "解除紧急退出" : "紧急退出";
    }
  }

  async function pushControl(command, value) {
    const res = await fetch("/api/control", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ command, value }),
    });
    if (!res.ok) {
      throw new Error(`control request failed: ${res.status}`);
    }
    const payload = await res.json();
    renderControlState(payload || {});
  }

  async function runOperator(command, extra = {}) {
    const res = await fetch("/api/operator", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ command, ...extra }),
    });
    const payload = await res.json().catch(() => ({}));
    if (!res.ok || !payload || payload.ok !== true) {
      throw new Error(`operator request failed: ${res.status}`);
    }
    return payload;
  }

  function bindControlActions() {
    const pauseBtn = $("btn-pause-opening");
    if (pauseBtn) {
      pauseBtn.addEventListener("click", async () => {
        setButtonBusy(pauseBtn, true);
        try {
          await pushControl("pause_opening", !controlState.pause_opening);
        } catch (_err) {
          // keep current state if control API is unavailable
        } finally {
          setButtonBusy(pauseBtn, false);
        }
      });
    }

    const reduceBtn = $("btn-reduce-only");
    if (reduceBtn) {
      reduceBtn.addEventListener("click", async () => {
        setButtonBusy(reduceBtn, true);
        try {
          await pushControl("reduce_only", !controlState.reduce_only);
        } catch (_err) {
          // keep current state if control API is unavailable
        } finally {
          setButtonBusy(reduceBtn, false);
        }
      });
    }

    const emergencyBtn = $("btn-emergency-stop");
    if (emergencyBtn) {
      emergencyBtn.addEventListener("click", async () => {
        setButtonBusy(emergencyBtn, true);
        try {
          await pushControl("emergency_stop", !controlState.emergency_stop);
        } catch (_err) {
          // keep current state if control API is unavailable
        } finally {
          setButtonBusy(emergencyBtn, false);
        }
      });
    }
  }

  async function refresh() {
    try {
      const [data, monitor30m, monitor12h, reconciliationEod] = await Promise.all([
        fetchJson("/api/state", null),
        fetchJson("/api/monitor/30m", EMPTY_MONITOR_REPORT("monitor_30m")),
        fetchJson("/api/monitor/12h", EMPTY_MONITOR_REPORT("monitor_12h")),
        fetchJson("/api/reconciliation/eod", EMPTY_RECONCILIATION_EOD_REPORT),
      ]);
      if (!data) return;

      const summary = data.summary || {};
      const config = data.config || {};
      const control = data.control || {};
      const now = Number(data.ts || 0);
      const pollSec = Number(config.poll_interval_seconds || 0);
      const stateAgeSec = Math.max(0, Math.floor(Date.now() / 1000 - now));
      const slotUtil = Number(summary.slot_utilization_pct || summary.exposure_pct || 0);
      const mode = String(config.execution_mode || (config.dry_run ? "paper" : "live")).toLowerCase();
      const brokerName = String(config.broker_name || (mode === "live" ? "LiveClobBroker" : "PaperBroker"));
      const opsGate = computeOpsGate(data, monitor30m, monitor12h, reconciliationEod);
      renderControlState(control);
      updateModeBadge(config);
      renderOpsGate(opsGate);

      if ($("status-line")) {
        $("status-line").textContent = `聪明钱包跟单账本 · 状态时间 ${hhmm(now)} · bot 轮询 ${pollSec}s · 前端刷新 ${FRONTEND_REFRESH_SECONDS}s`;
      }

      if ($("guard-line")) {
        const basePerTrade = Number(summary.base_per_trade_notional || summary.per_trade_notional || 0);
        const maxPerTrade = Number(summary.theoretical_max_order_notional || 0);
        $("guard-line").textContent = `${mode.toUpperCase()} · ${brokerName} · 基准单笔 ${basePerTrade.toFixed(2)}U · 理论上限 ${maxPerTrade.toFixed(2)}U`;
      }

      if ($("budget-meta")) {
        const trackedNotional = Number(summary.tracked_notional_usd || 0);
        const availableNotional = Number(summary.available_notional_usd || config.bankroll_usd || 0);
        $("budget-meta").textContent = `槽位 ${Number(summary.open_positions || 0)}/${Number(summary.max_open_positions || 0)} · 已用 ${trackedNotional.toFixed(2)}U / 可用 ${availableNotional.toFixed(2)}U`;
      }
      if ($("budget-fill")) {
        $("budget-fill").style.width = `${Math.max(0, Math.min(100, slotUtil)).toFixed(1)}%`;
      }

      if ($("kpi-mode")) $("kpi-mode").textContent = mode.toUpperCase();
      if ($("kpi-mode-note")) {
        $("kpi-mode-note").textContent = mode === "live" ? `${brokerName} · 会发送真实订单` : `${brokerName} · 不会发送真实订单`;
      }
      if ($("kpi-pos")) $("kpi-pos").textContent = `${Number(summary.open_positions || 0)} / ${Number(summary.max_open_positions || 0)}`;
      if ($("kpi-slot-note")) $("kpi-slot-note").textContent = `槽位利用率 ${fmtPct(slotUtil, 2)}`;
      if ($("kpi-notional")) $("kpi-notional").textContent = fmtUsd(summary.tracked_notional_usd || 0, false);
      if ($("kpi-notional-note")) {
        const available = Number(summary.available_notional_usd || 0);
        $("kpi-notional-note").textContent = `每轮信号 ${Number(summary.signals || 0)} · 可用预算 ${available.toFixed(2)}U`;
      }
      if ($("kpi-risk-mode")) {
        $("kpi-risk-mode").textContent = slotUtil >= Number(config.congested_utilization_threshold || 1) * 100 ? "拥堵自适应" : "常规风控";
      }
      if ($("kpi-risk-note")) {
        $("kpi-risk-note").textContent = `拥堵阈值 ${fmtPct(Number(config.congested_utilization_threshold || 0) * 100, 0)} / 拥堵减仓 ${fmtPct(Number(config.congested_trim_pct || 0) * 100, 0)}`;
      }

      updateStrategyParams(config);

      const positions = (data.positions || []).slice(0, 8);
      replaceRows(
        $("positions-body"),
        positions.map((p) => {
          const holdMin = Math.max(0, Math.floor((Date.now() / 1000 - Number(p.opened_ts || now)) / 60));
          const lastExitLabel = String(p.last_exit_label || "").trim();
          const lastExitSummary = String(p.last_exit_summary || "").trim();
          const lastExitTs = Number(p.last_exit_ts || 0);
          const [lastExitCls, lastExitTag] = exitTagMeta(p.last_exit_kind, lastExitLabel);
          const exitDetail = lastExitLabel
            ? `<div class="cell-stack">
                <span class="cell-main">${p.exit_rule || "-"}</span>
                <span class="cell-sub"><span class="tag ${lastExitCls}">${lastExitTag}</span>${lastExitSummary ? ` ${lastExitSummary}` : ""}${lastExitTs > 0 ? ` · ${historyAgeLabel(lastExitTs, now)}` : ""}</span>
              </div>`
            : `<div class="cell-stack"><span class="cell-main">${p.exit_rule || "-"}</span><span class="cell-sub">${Array.isArray(p.exit_modes) ? p.exit_modes.join(" / ") : ""}</span></div>`;
          const traceId = String(p.trace_id || "");
          const rowClass = traceId && traceId === selectedTraceId ? "click-row active-row" : (traceId ? "click-row" : "");
          return `<tr>
            <td>${p.title || p.market_slug || "-"}</td>
            <td>${p.outcome || "YES"}</td>
            <td>${Number(p.quantity || 0).toFixed(2)}</td>
            <td>${fmtUsd(p.notional || 0, false)}</td>
            <td>${Number(p.book_price || 0).toFixed(4)}</td>
            <td>${fmtHoldMinutes(holdMin)}</td>
            <td>${p.reason || "-"}</td>
            <td class="wrap">${exitDetail}</td>
          </tr>`.replace("<tr>", `<tr class="${rowClass}" data-trace-id="${attrToken(traceId)}">`);
        }),
        '<tr><td colspan="8">暂无持仓</td></tr>'
      );

      const orders = (data.orders || []).slice(0, 20);
      updateOrderChips(orders);
      replaceRows(
        $("orders-body"),
        orders.slice(0, 10).map((o) => {
          const st = String(o.status || "PENDING").toUpperCase();
          const map = { FILLED: ["ok", "已成交"], PENDING: ["wait", "待成交"], REJECTED: ["danger", "已拒绝"], CANCELED: ["cancel", "已撤单"], CLEARED: ["cancel", "已清理"] };
          const [cls, txt] = map[st] || ["wait", st];
          const ts = hhmm(Number(o.ts || now));
          const [action, actionLabel] = orderActionMeta(o);
          const [actionCls, actionTag] = actionTagMeta(action, actionLabel);
          const exitLabel = String(o.exit_label || "").trim();
          const [exitCls, exitTag] = exitTagMeta(o.exit_kind, exitLabel);
          const reasonText = String(o.reason || "-");
          const summaryText = String(o.exit_summary || "").trim();
          const reasonCell = exitLabel
            ? `<div class="cell-stack" title="${reasonText}">
                <span><span class="tag ${exitCls}">${exitTag}</span></span>
                <span class="cell-sub">${summaryText || reasonText}</span>
              </div>`
            : reasonText;
          return `<tr>
            <td class="mono">${ts}</td>
            <td>${o.title || "-"}</td>
            <td><span class="tag ${actionCls}">${actionTag}</span></td>
            <td><span class="tag ${cls}">${txt}</span></td>
            <td class="wrap">${reasonCell}</td>
          </tr>`;
        }),
        '<tr><td colspan="5">暂无订单</td></tr>'
      );

      const wallets = (data.wallets || []).slice(0, 8);
      const availableWallets = wallets
        .map((w) => normalizeWallet(w.wallet))
        .filter((wallet) => !!wallet);
      if (!availableWallets.includes(selectedWallet)) {
        selectedWallet = availableWallets[0] || "";
      }
      replaceRows(
        $("wallets-body"),
        wallets.map((w) => {
          const score = Number(w.score || 0);
          const tier = String(w.tier || "LOW").toUpperCase();
          const history = historyProfile(w, config);
          const scoreSummary = String(w.score_summary || "").trim();
          const discoverySummary = String(w.discovery_priority_reason || "").trim();
          const scoreDetail = `${Number(w.positions || 0)} pos · ${Number(w.unique_markets || 0)} mkts · ${Number(w.notional || 0).toFixed(0)}U`;
          const walletKey = normalizeWallet(w.wallet);
          const rowClass = walletKey && walletKey === selectedWallet ? "click-row active-row" : "click-row";
          return `<tr>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${shortWallet(w.wallet)}</span>
                <span class="cell-sub">${scoreDetail}</span>
              </div>
            </td>
            <td class="wrap" title="${scoreSummary}">
              <div class="cell-stack">
                <span class="${clsForScore(score)} cell-main">${score.toFixed(1)}</span>
                <span><span class="tag ${tierTagClass(tier)}">${tier}</span></span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span><span class="tag ${history.cls}">${history.label}</span></span>
                <span class="cell-sub">${history.detail} · ${historyAgeLabel(w.history_refresh_ts, now)}</span>
              </div>
            </td>
            <td class="${clsForValue(w.roi)}">${fmtSignedRatioPct(w.roi, 1)}</td>
            <td>${fmtRatioPct(w.win_rate, 1)}</td>
            <td>${Number(w.resolved_markets || 0) > 0 ? fmtRatioPct(w.resolved_win_rate, 1) : '<span class="muted">--</span>'}</td>
            <td title="${discoverySummary || scoreSummary}">${fmtPct(Number(w.top_market_share || 0) * 100, 1)}</td>
          </tr>`.replace("<tr>", `<tr class="${rowClass}" data-wallet="${walletKey}">`);
        }),
        '<tr><td colspan="7">暂无钱包数据</td></tr>'
      );

      const sources = (data.sources || []).slice(0, 8);
      replaceRows(
        $("sources-body"),
        sources.map((s) => {
          const history = historyProfile(s, config);
          const score = Number(s.score || 0);
          const tier = String(s.tier || "LOW").toUpperCase();
          const walletKey = normalizeWallet(s.name);
          const rowClass = walletKey && walletKey === selectedWallet ? "click-row active-row" : "click-row";
          const historyText = Number(s.closed_positions || 0) > 0
            ? `W ${fmtRatioPct(s.win_rate, 0)} / ROI ${fmtSignedRatioPct(s.roi, 0)}`
            : "暂无结算历史";
          const discoveryText = Number(s.discovery_priority_score || 0) > 0
            ? `pool #${Number(s.discovery_priority_rank || 0)} · ${Number(s.discovery_priority_score || 0).toFixed(2)}`
            : "pool --";
          return `<tr>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${shortWallet(s.name)}</span>
                <span class="cell-sub">${Number(s.positions || 0)} pos · ${Number(s.unique_markets || 0)} mkts</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span class="${clsForScore(score)} cell-main">${score.toFixed(1)}</span>
                <span><span class="tag ${tierTagClass(tier)}">${tier}</span></span>
              </div>
            </td>
            <td class="${clsForWeight(Number(s.weight || 0))}">${Number(s.weight || 0).toFixed(2)}</td>
            <td class="wrap">
              <div class="cell-stack">
                <span><span class="tag ${history.cls}">${history.label}</span></span>
                <span class="cell-sub">${historyText}${Number(s.resolved_markets || 0) > 0 ? ` · resolved ${fmtRatioPct(s.resolved_win_rate, 0)}` : ""}</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${s.updated || "-"}</span>
                <span class="cell-sub">hist ${historyAgeLabel(s.history_refresh_ts, now)} · ${discoveryText}</span>
              </div>
            </td>
          </tr>`.replace("<tr>", `<tr class="${rowClass}" data-wallet="${walletKey}">`);
        }),
        '<tr><td colspan="5">暂无来源数据</td></tr>'
      );

      const selectedWalletData = wallets.find((wallet) => normalizeWallet(wallet.wallet) === selectedWallet) || null;
      renderWalletDetail(selectedWalletData, config, now);
      renderExitReview(data.exit_review || {}, now);
      renderSignalReview(data.signal_review || {}, now);
      renderAttributionReview(data.attribution_review || {}, now);
      renderMonitorReports(monitor30m, monitor12h, reconciliationEod, now);
      renderDiagnostics(data, monitor30m, monitor12h, reconciliationEod, now);

      const alerts = (data.alerts || []).slice(0, 6);
      if ($("alerts-list")) {
        $("alerts-list").innerHTML = alerts.length > 0
          ? alerts.map((a) => `<li><span class="level ${a.cls}">${a.tag}</span><span>${a.message}</span></li>`).join("")
          : '<li><span class="level green">正常</span><span>暂无异常告警</span></li>';
      }

      const timeline = (data.timeline || []).slice(0, 8);
      if ($("timeline-list")) {
        $("timeline-list").innerHTML = timeline.length > 0
          ? timeline.map((t) => {
              const [actionCls, actionTag] = actionTagMeta(t.action, t.action_label);
              const status = String(t.status || "").toUpperCase();
              const rawText = String(t.text || "");
              const actionPrefix = String(t.action_label || "").trim();
              const titleText = actionPrefix && rawText.startsWith(`${actionPrefix} `)
                ? rawText.slice(actionPrefix.length + 1)
                : rawText;
              const [statusCls, statusTag] = status === "FILLED"
                ? ["ok", "已成交"]
                : status === "REJECTED"
                  ? ["danger", "已拒绝"]
                  : status
                    ? ["wait", status]
                    : ["cancel", "未记录"];
              return `<li><span>${t.time}</span><b><span class="tag ${actionCls}">${actionTag}</span> <span class="tag ${statusCls}">${statusTag}</span> ${titleText || rawText || ""}</b></li>`;
            }).join("")
          : '<li><span>--:--</span><b>暂无时间轴事件</b></li>';
      }

      updateRiskMeters(summary, orders, config, stateAgeSec);
      if ($("health-runtime")) $("health-runtime").textContent = `状态 ${fmtAge(stateAgeSec)} · bot ${pollSec}s`;
      if ($("health-state-age")) $("health-state-age").textContent = `${stateAgeSec}s`;
      if ($("health-broker")) $("health-broker").textContent = brokerName;
      if ($("health-wallet-pool")) $("health-wallet-pool").textContent = String(config.wallet_pool_size || 0);
      if ($("health-discovery")) {
        const enabled = config.wallet_discovery_enabled ? "on" : "off";
        $("health-discovery").textContent = `${String(config.wallet_discovery_mode || "-")} / ${enabled}`;
      }
      if ($("health-control")) $("health-control").textContent = controlLabel();
    } catch (_err) {
      // keep static fallback
    }
  }

  loadUiState();
  bindControlActions();
  bindOpsGateActions();
  bindWalletSelection();
  bindExitReviewFilters();
  bindExitReviewSamples();
  bindTraceReviewInteractions();
  bindAttributionWindows();
  bindDiagnosticsInteractions();
  refresh();
  setInterval(refresh, FRONTEND_REFRESH_SECONDS * 1000);
})();
