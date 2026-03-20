(function () {
  const $ = (id) => document.getElementById(id);
  const FRONTEND_REFRESH_SECONDS = 5;
  const controlState = {
    decision_mode: "manual",
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
    selectedCandidateId: "poly.selectedCandidateId",
    candidateQueueFilter: "poly.candidateQueueFilter",
    candidateFiltersExpanded: "poly.candidateFiltersExpanded",
    workspaceView: "poly.workspaceView",
    candidateFocusView: "poly.candidateFocusView",
  };
  const CANDIDATE_FILTER_DEFAULTS = {
    search: "",
    status: "all",
    action: "all",
    side: "all",
    sort: "score_desc",
  };
  let selectedWallet = "";
  let selectedTraceId = "";
  let selectedSignalCycleId = "";
  let selectedAttributionWindow = "24h";
  let selectedCandidateId = "";
  let candidateQueueFilter = { ...CANDIDATE_FILTER_DEFAULTS };
  let candidateFiltersExpanded = false;
  let workspaceView = "overview";
  let candidateFocusView = "summary";
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
  const EMPTY_DECISION_MODE = {
    mode: "manual",
    updated_ts: 0,
    updated_by: "",
    note: "",
    available_modes: ["manual", "semi_auto", "auto"],
  };
  const EMPTY_CANDIDATES = {
    summary: {
      count: 0,
      pending: 0,
      approved: 0,
      watched: 0,
      executed: 0,
    },
    items: [],
  };
  const EMPTY_CANDIDATE_DETAIL = {
    candidate: {},
    related_actions: [],
    related_journal: [],
    trace: {},
    related_cycles: [],
    decision_chain: [],
    orders: [],
    timeline: [],
    summary: {
      related_action_count: 0,
      related_journal_count: 0,
      order_count: 0,
      decision_chain_count: 0,
      cycle_count: 0,
      trace_found: false,
    },
  };
  const EMPTY_WALLET_PROFILES = {
    summary: {
      count: 0,
      enabled: 0,
    },
    items: [],
  };
  const EMPTY_NOTIFIER = {
    local_available: false,
    webhook_configured: false,
    telegram_configured: false,
    channels: [],
    delivery_stats: {
      event_count: 0,
      delivery_count: 0,
      ok_events: 0,
      failed_events: 0,
      by_channel: {},
    },
    recent: [],
    last: {},
    updated_ts: 0,
  };
  const EMPTY_JOURNAL = {
    days: 30,
    total_entries: 0,
    execution_actions: 0,
    watch_actions: 0,
    ignore_actions: 0,
    updated_ts: 0,
    recent: [],
    notes: [],
  };
  let lastDecisionCandidates = EMPTY_CANDIDATES;
  let lastDecisionMode = EMPTY_DECISION_MODE;
  let lastDecisionApiState = {
    candidates: { ok: true, error: "" },
    mode: { ok: true, error: "" },
  };
  let lastCandidateDetail = EMPTY_CANDIDATE_DETAIL;
  let lastCandidateDetailApiState = { ok: true, error: "", candidateId: "", pending: false };
  let candidateDetailRequestSeq = 0;
  let lastWalletProfiles = EMPTY_WALLET_PROFILES;
  let lastWalletProfilesApiState = { ok: true, error: "" };
  let lastNotifierSummary = EMPTY_NOTIFIER;
  let lastJournalSummary = EMPTY_JOURNAL;
  let lastJournalApiState = { ok: true, error: "" };
  const candidateRequestState = Object.create(null);
  const walletProfileDrafts = Object.create(null);
  const walletProfileRequestState = Object.create(null);
  let decisionConsoleNotice = { cls: "wait", text: "等待 candidates / mode 数据..." };
  let journalComposerNotice = { cls: "wait", text: "支持快速写一句理由" };

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
      selectedCandidateId = String(window.localStorage.getItem(STORAGE_KEYS.selectedCandidateId) || "");
      candidateFiltersExpanded = window.localStorage.getItem(STORAGE_KEYS.candidateFiltersExpanded) === "1";
      workspaceView = String(window.localStorage.getItem(STORAGE_KEYS.workspaceView) || "overview");
      candidateFocusView = String(window.localStorage.getItem(STORAGE_KEYS.candidateFocusView) || "summary");
      if (!["overview", "wallets", "ops", "review"].includes(workspaceView)) workspaceView = "overview";
      if (!["summary", "detail"].includes(candidateFocusView)) candidateFocusView = "summary";
      const rawCandidateFilter = window.localStorage.getItem(STORAGE_KEYS.candidateQueueFilter);
      if (rawCandidateFilter) {
        const parsed = JSON.parse(rawCandidateFilter);
        if (parsed && typeof parsed === "object") {
          candidateQueueFilter = {
            search: String(parsed.search || "").trim(),
            status: String(parsed.status || "all").trim() || "all",
            action: String(parsed.action || "all").trim() || "all",
            side: String(parsed.side || "all").trim() || "all",
            sort: String(parsed.sort || "score_desc").trim() || "score_desc",
          };
        }
      }
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
      window.localStorage.setItem(STORAGE_KEYS.selectedCandidateId, String(selectedCandidateId || ""));
      window.localStorage.setItem(STORAGE_KEYS.candidateFiltersExpanded, candidateFiltersExpanded ? "1" : "0");
      window.localStorage.setItem(STORAGE_KEYS.workspaceView, String(workspaceView || "overview"));
      window.localStorage.setItem(STORAGE_KEYS.candidateFocusView, String(candidateFocusView || "summary"));
      window.localStorage.setItem(
        STORAGE_KEYS.candidateQueueFilter,
        JSON.stringify({
          search: String(candidateQueueFilter.search || "").trim(),
          status: String(candidateQueueFilter.status || "all").trim() || "all",
          action: String(candidateQueueFilter.action || "all").trim() || "all",
          side: String(candidateQueueFilter.side || "all").trim() || "all",
          sort: String(candidateQueueFilter.sort || "score_desc").trim() || "score_desc",
        })
      );
    } catch (_err) {
      // ignore storage failures
    }
  }

  function fmtUsd(n, showSign = true) {
    const v = Number(n || 0);
    const sign = showSign && v > 0 ? "+" : "";
    return `${sign}$${v.toFixed(2)}`;
  }

  function workspaceViewLabel(value) {
    const key = String(value || "").trim().toLowerCase();
    if (key === "wallets") return "钱包";
    if (key === "ops") return "监控";
    if (key === "review") return "复盘";
    return "总览";
  }

  function candidateFocusViewLabel(value) {
    return String(value || "").trim().toLowerCase() === "detail" ? "完整复盘" : "候选解读";
  }

  function renderWorkspaceShell() {
    const root = document.querySelector(".console");
    if (root) root.dataset.workspaceView = workspaceView;
    const metaEl = $("workspace-meta");
    if (metaEl) {
      const copy = {
        overview: "当前视图：总览，聚焦当下决策、持仓、订单和最关键告警。",
        wallets: "当前视图：钱包，集中看核心钱包池、画像、评分与来源质量。",
        ops: "当前视图：监控，集中看 startup、monitor、对账、风险和运行状态。",
        review: "当前视图：复盘，集中看归档、日记、信号回放、归因和退出样本。",
      };
      metaEl.textContent = copy[workspaceView] || copy.overview;
    }
    const nav = $("workspace-nav");
    if (nav) {
      nav.querySelectorAll("[data-workspace-view]").forEach((button) => {
        button.classList.toggle("active", String(button.getAttribute("data-workspace-view") || "") === workspaceView);
      });
    }
  }

  function renderCandidateFocusViewSwitch() {
    const switchEl = $("candidate-focus-view-switch");
    if (switchEl) {
      switchEl.querySelectorAll("[data-candidate-focus-view]").forEach((button) => {
        button.classList.toggle("active", String(button.getAttribute("data-candidate-focus-view") || "") === candidateFocusView);
      });
    }
    document.querySelectorAll("[data-candidate-focus-view-panel]").forEach((panel) => {
      panel.classList.toggle("active", String(panel.getAttribute("data-candidate-focus-view-panel") || "") === candidateFocusView);
    });
  }

  function renderCandidateFilterToggle() {
    const filtersEl = $("candidate-toolbar-filters");
    if (filtersEl) {
      filtersEl.classList.toggle("is-expanded", !!candidateFiltersExpanded);
    }
    const toggleBtn = $("candidate-toggle-filters");
    if (toggleBtn) {
      toggleBtn.textContent = candidateFiltersExpanded ? "收起筛选" : "展开筛选";
      toggleBtn.classList.toggle("active", !!candidateFiltersExpanded);
      toggleBtn.setAttribute("aria-expanded", candidateFiltersExpanded ? "true" : "false");
    }
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
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

  function csvEscape(value) {
    const text = String(value ?? "");
    if (/[",\n]/.test(text)) {
      return `"${text.replaceAll('"', '""')}"`;
    }
    return text;
  }

  function downloadText(filename, content, mime = "text/plain;charset=utf-8") {
    const blob = new Blob([String(content ?? "")], { type: mime });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    link.style.display = "none";
    document.body.appendChild(link);
    link.click();
    window.setTimeout(() => {
      window.URL.revokeObjectURL(url);
      link.remove();
    }, 0);
  }

  function downloadJson(filename, payload) {
    downloadText(filename, JSON.stringify(payload ?? {}, null, 2), "application/json;charset=utf-8");
  }

  function downloadCsv(filename, rows, columns) {
    const header = (columns || []).map((col) => csvEscape(col.label || col.key)).join(",");
    const body = (Array.isArray(rows) ? rows : [])
      .map((row) => (columns || []).map((col) => csvEscape(col.value(row))).join(","))
      .join("\n");
    downloadText(filename, [header, body].filter(Boolean).join("\n"), "text/csv;charset=utf-8");
  }

  function candidateKey(candidate) {
    return String(candidate && (candidate.id || candidate.signal_id || candidate.trace_id || candidate.market_slug || "") || "");
  }

  function selectedCandidate(payload) {
    const items = Array.isArray(payload && payload.items) ? payload.items : [];
    if (items.length <= 0) return null;
    const target = String(selectedCandidateId || "");
    const selected = items.find((candidate) => candidateKey(candidate) === target);
    return selected || items[0] || null;
  }

  function filteredCandidatePayload(payload) {
    const source = payload && typeof payload === "object" ? payload : EMPTY_CANDIDATES;
    const allItems = Array.isArray(source.items) ? source.items.slice(0, 32) : [];
    const filteredItems = candidateSortCandidates(
      allItems.filter((candidate) => candidateMatchesQueueFilter(candidate, candidateQueueFilter)),
      candidateQueueFilter.sort
    );
    return {
      allItems,
      filteredItems,
      visiblePayload: { ...source, items: filteredItems },
    };
  }

  function candidateDetailMatches(candidate, detailPayload) {
    if (!candidate || typeof candidate !== "object" || !detailPayload || typeof detailPayload !== "object") return false;
    const detailCandidate = detailPayload.candidate && typeof detailPayload.candidate === "object" ? detailPayload.candidate : {};
    const detailId = candidateKey(detailCandidate);
    const currentId = candidateKey(candidate);
    if (detailId && currentId && detailId === currentId) return true;
    const detailTrace = String(detailCandidate.trace_id || detailPayload.trace && detailPayload.trace.trace_id || "").trim();
    const currentTrace = String(candidate.trace_id || "").trim();
    if (detailTrace && currentTrace && detailTrace === currentTrace) return true;
    const detailSignal = String(detailCandidate.signal_id || "").trim();
    const currentSignal = String(candidate.signal_id || "").trim();
    if (detailSignal && currentSignal && detailSignal === currentSignal) return true;
    return false;
  }

  async function loadCandidateDetail(candidateId, { force = false } = {}) {
    const normalizedId = String(candidateId || "").trim();
    const currentDetailId = String(lastCandidateDetailApiState.candidateId || "").trim();
    if (!normalizedId) {
      lastCandidateDetail = EMPTY_CANDIDATE_DETAIL;
      lastCandidateDetailApiState = { ok: true, error: "", candidateId: "", pending: false };
      return lastCandidateDetail;
    }
    if (
      !force &&
      !lastCandidateDetailApiState.pending &&
      currentDetailId === normalizedId &&
      candidateDetailMatches({ id: normalizedId, signal_id: normalizedId, trace_id: normalizedId }, lastCandidateDetail)
    ) {
      return lastCandidateDetail;
    }
    const requestSeq = ++candidateDetailRequestSeq;
    lastCandidateDetailApiState = {
      ok: lastCandidateDetailApiState.ok,
      error: "",
      candidateId: normalizedId,
      pending: true,
    };
    const result = await fetchJsonState(`/api/candidates/${encodeURIComponent(normalizedId)}`, EMPTY_CANDIDATE_DETAIL);
    if (requestSeq !== candidateDetailRequestSeq) return lastCandidateDetail;
    lastCandidateDetail = result && result.data ? result.data : EMPTY_CANDIDATE_DETAIL;
    lastCandidateDetailApiState = {
      ok: !!(result && result.ok),
      error: String(result && result.error || ""),
      candidateId: normalizedId,
      pending: false,
    };
    return lastCandidateDetail;
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

  function recommendationLabel(text, fallbackStatus = "") {
    return reportDecisionMeta(text, fallbackStatus)[1];
  }

  function reportGeneratedTs(report) {
    const payload = report && typeof report === "object" ? report : {};
    return Number(payload.generated_ts || 0);
  }

  function reportFreshnessMeta(report, windowSeconds, now) {
    const generatedTs = reportGeneratedTs(report);
    const currentTs = Number(now || 0);
    const ageSeconds = generatedTs > 0 && currentTs > 0 ? Math.max(0, currentTs - generatedTs) : 0;
    const windowSec = Math.max(0, Number(windowSeconds || 0));
    const staleThreshold = windowSec > 0
      ? Math.max(600, windowSec + Math.max(600, Math.floor(windowSec / 10)))
      : 3600;
    const stale = generatedTs <= 0 || ageSeconds >= staleThreshold;
    return {
      generatedTs,
      ageSeconds,
      ageLabel: generatedTs > 0 ? historyAgeLabel(generatedTs, currentTs || generatedTs) : "未生成",
      stale,
    };
  }

  function inconclusiveWindowLabel(report) {
    const payload = report && typeof report === "object" ? report : {};
    const count = Number(payload.consecutive_inconclusive_windows || 0);
    return count > 0 ? `INCONCLUSIVE x${count}` : "INCONCLUSIVE";
  }

  function monitorWindowSummary(report) {
    const payload = report && typeof report === "object" ? report : {};
    const finalText = String(payload.final_recommendation || "").trim();
    const rawText = String(payload.recommendation || "").trim();
    const sampleStatus = String(payload.sample_status || "").trim().toUpperCase();
    const finalLabel = recommendationLabel(finalText, payload.reconciliation_status || payload.sample_status || "unknown");
    const rawLabel = sampleStatus === "INCONCLUSIVE"
      ? inconclusiveWindowLabel(payload)
      : rawText
        ? recommendationLabel(rawText, payload.sample_status || payload.reconciliation_status || "unknown")
        : "";
    if (finalText) {
      if (rawLabel && rawText !== finalText) return `${finalLabel} · ${rawLabel}`;
      return finalText;
    }
    if (rawLabel) return rawLabel;
    return sampleStatus || "unknown";
  }

  function monitorWindowDisplaySummary(report, windowSeconds, now) {
    const summary = monitorWindowSummary(report);
    const freshness = reportFreshnessMeta(report, windowSeconds, now);
    const suffix = freshness.ageLabel;
    return freshness.stale && freshness.generatedTs > 0
      ? `${summary} · ${suffix} · STALE`
      : `${summary} · ${suffix}`;
  }

  function reconciliationStatusSummary(reconciliation, eodReport) {
    const live = reconciliation && typeof reconciliation === "object" ? reconciliation : {};
    const eod = eodReport && typeof eodReport === "object" ? eodReport : {};
    const status = String(live.status || eod.status || "unknown");
    const liveIssues = Array.isArray(live.issues) ? live.issues.filter((item) => String(item || "").trim()) : [];
    const eodIssues = Array.isArray(eod.issues) ? eod.issues.filter((item) => String(item || "").trim()) : [];
    const issues = liveIssues.length > 0 ? liveIssues : eodIssues;
    return issues.length > 0 ? `${status} · ${String(issues[0])}` : status;
  }

  function modeLabel(mode) {
    const value = String(mode || "").trim().toLowerCase();
    if (value === "manual") return "手动";
    if (value === "semi_auto") return "半自动";
    if (value === "auto") return "自动";
    return value || "未知";
  }

  function candidateStatusMeta(status) {
    const value = String(status || "").trim().toLowerCase();
    if (value === "approved" || value === "queued") return ["wait", value === "queued" ? "已排队" : "已批准"];
    if (value === "watched") return ["cancel", "观察中"];
    if (value === "executed") return ["ok", "已执行"];
    if (value === "submitted") return ["wait", "已发单"];
    if (value === "rejected" || value === "risk_rejected") return ["danger", "已拒绝"];
    if (value === "ignored") return ["cancel", "已忽略"];
    if (value === "expired") return ["cancel", "已过期"];
    return ["wait", value ? value.toUpperCase() : "待处理"];
  }

  function candidateActionText(action, side) {
    const value = String(action || "").trim().toLowerCase();
    if (value === "ignore") return "忽略";
    if (value === "watch") return "观察";
    if (value === "buy_small") return "小仓跟随";
    if (value === "buy_normal") return "正常跟随";
    if (value === "follow") return side === "SELL" ? "立即跟随退出" : "直接跟随";
    if (value === "close_partial") return "减仓";
    if (value === "close_all") return "清仓";
    return value || "处理";
  }

  const CANDIDATE_ACTION_INTERACTIVE_STATUSES = new Set(["pending", "queued", "approved", "watched", "submitted"]);

  function candidateIsActionable(status) {
    const value = String(status || "pending").trim().toLowerCase();
    return CANDIDATE_ACTION_INTERACTIVE_STATUSES.has(value);
  }

  function candidatePrimaryAction(candidate, side) {
    const status = candidateStatusValue(candidate);
    const action = candidateActionValue(candidate);
    const actionValue = String(action || "").trim().toLowerCase();
    const normalizedSide = String(side || "BUY").trim().toUpperCase();
    if (!candidateIsActionable(status)) return "";
    if (actionValue && actionValue !== "watch" && actionValue !== "ignore") return actionValue;
    return normalizedSide === "SELL" ? "close_all" : "follow";
  }

  function candidateCardActionButtons(candidate, side) {
    const status = candidateStatusValue(candidate);
    const canOperate = candidateIsActionable(status);
    const primary = canOperate ? candidatePrimaryAction(candidate, side) : "";
    if (!canOperate) {
      return {
        canOperate,
        primary,
        secondary: [],
      };
    }

    const actions = [
      { action: "watch", label: "观察", cls: "subtle" },
      { action: "ignore", label: "忽略", cls: "ghost" },
    ];
    const filtered = [];
    for (const item of actions) {
      const shouldHide = primary && String(item.action || "") === String(primary || "");
      if (shouldHide) continue;
      filtered.push(item);
    }
    return {
      canOperate,
      primary,
      secondary: filtered,
    };
  }

  function candidateStatusValue(candidate) {
    const values = [
      candidate && candidate.review_status,
      candidate && candidate.status,
      candidate && candidate.final_status,
    ];
    for (const raw of values) {
      const value = String(raw || "").trim().toLowerCase();
      if (!value) continue;
      if (["pending", "approved", "watched", "executed", "ignored", "rejected", "expired", "queued"].includes(value)) {
        return value;
      }
    }
    return "pending";
  }

  function candidateActionValue(candidate) {
    const values = [
      candidate && candidate.review_action,
      candidate && candidate.selected_action,
      candidate && candidate.suggested_action,
      candidate && candidate.action,
    ];
    for (const raw of values) {
      const value = String(raw || "").trim().toLowerCase();
      if (value) return value;
    }
    return "";
  }

  function candidateSearchText(candidate) {
    if (!candidate || typeof candidate !== "object") return "";
    const values = [
      candidate.id,
      candidate.signal_id,
      candidate.trace_id,
      candidate.wallet,
      candidate.wallet_tag,
      candidate.wallet_tier,
      candidate.market_slug,
      candidate.token_id,
      candidate.condition_id,
      candidate.outcome,
      candidate.side,
      candidate.trigger_type,
      candidate.market_tag,
      candidate.resolution_bucket,
      candidate.suggested_action,
      candidate.selected_action,
      candidate.review_action,
      candidate.status,
      candidate.review_status,
      candidate.final_status,
      candidate.skip_reason,
      candidate.recommendation_reason,
      candidate.note,
      candidate.wallet_score_summary,
      candidate.action_label,
      candidate.review_note,
    ];
    const reasonFactors = Array.isArray(candidate.reason_factors) ? candidate.reason_factors : [];
    for (const factor of reasonFactors) {
      if (!factor || typeof factor !== "object") continue;
      values.push(factor.key, factor.label, factor.value, factor.detail);
    }
    const explain = Array.isArray(candidate.explanation) ? candidate.explanation : [];
    for (const item of explain) {
      if (!item || typeof item !== "object") continue;
      values.push(item.label, item.value);
    }
    const snapshots = [
      candidate.signal_snapshot,
      candidate.topic_snapshot,
      candidate.decision_snapshot,
      candidate.order_snapshot,
      candidate.position_snapshot,
    ];
    for (const snap of snapshots) {
      if (!snap || typeof snap !== "object") continue;
      for (const [key, value] of Object.entries(snap)) {
        values.push(key, value);
      }
    }
    return values
      .flatMap((value) => {
        if (value == null) return [];
        if (Array.isArray(value)) return value;
        if (typeof value === "object") return [JSON.stringify(value)];
        return [String(value)];
      })
      .join(" ")
      .replace(/\s+/g, " ")
      .trim()
      .toLowerCase();
  }

  function candidateMatchesQueueFilter(candidate, filters) {
    const search = String(filters && filters.search || "").trim().toLowerCase();
    if (search) {
      const haystack = candidateSearchText(candidate);
      if (!haystack.includes(search)) return false;
    }
    const statusFilter = String(filters && filters.status || "all").trim().toLowerCase();
    if (statusFilter && statusFilter !== "all" && candidateStatusValue(candidate) !== statusFilter) return false;
    const actionFilter = String(filters && filters.action || "all").trim().toLowerCase();
    if (actionFilter && actionFilter !== "all") {
      const actionValue = candidateActionValue(candidate);
      if (actionValue !== actionFilter) return false;
    }
    const sideFilter = String(filters && filters.side || "all").trim().toUpperCase();
    if (sideFilter && sideFilter !== "ALL" && String(candidate && candidate.side || "").trim().toUpperCase() !== sideFilter) return false;
    return true;
  }

  function candidateSortValue(candidate, key) {
    const valueKey = String(key || "score_desc").trim().toLowerCase();
    if (valueKey === "freshness_desc") return Number(candidate.updated_ts || candidate.created_ts || candidate.signal_ts || 0);
    if (valueKey === "confidence_desc") return Number(candidate.confidence || 0);
    if (valueKey === "wallet_score_desc") return Number(candidate.wallet_score || 0);
    if (valueKey === "observed_notional_desc") return Number(candidate.observed_notional || 0);
    return Number(candidate.score || 0);
  }

  function candidateSortCandidates(items, key) {
    const sortKey = String(key || "score_desc").trim().toLowerCase();
    const desc = true;
    return Array.isArray(items)
      ? items.slice().sort((left, right) => {
          const leftValue = candidateSortValue(left, sortKey);
          const rightValue = candidateSortValue(right, sortKey);
          if (rightValue !== leftValue) return desc ? rightValue - leftValue : leftValue - rightValue;
          const leftTs = Number(left.updated_ts || left.created_ts || 0);
          const rightTs = Number(right.updated_ts || right.created_ts || 0);
          if (rightTs !== leftTs) return rightTs - leftTs;
          return String(candidateKey(left)).localeCompare(String(candidateKey(right)));
        })
      : [];
  }

  function candidateSortLabel(key) {
    const value = String(key || "").trim().toLowerCase();
    if (value === "freshness_desc") return "新鲜度";
    if (value === "confidence_desc") return "置信度";
    if (value === "wallet_score_desc") return "钱包分";
    if (value === "observed_notional_desc") return "观察金额";
    return "综合评分";
  }

  function candidateFilterLabel(kind, value) {
    const rawKind = String(kind || "").trim().toLowerCase();
    const rawValue = String(value || "all").trim().toLowerCase();
    if (rawValue === "all" || !rawValue) return "全部";
    if (rawKind === "status") {
      const map = {
        pending: "待处理",
        approved: "已批准",
        watched: "观察中",
        executed: "已执行",
        ignored: "已忽略",
        rejected: "已拒绝",
        expired: "已过期",
        queued: "已排队",
      };
      return map[rawValue] || rawValue;
    }
    if (rawKind === "action") {
      const map = {
        ignore: "忽略",
        watch: "观察",
        buy_small: "小仓",
        buy_normal: "正常",
        follow: "跟随",
        close_partial: "减仓",
        close_all: "清仓",
      };
      return map[rawValue] || rawValue;
    }
    if (rawKind === "side") {
      if (rawValue === "buy") return "BUY";
      if (rawValue === "sell") return "SELL";
      return rawValue.toUpperCase();
    }
    return rawValue;
  }

  function candidateFilterSummary(filters, visibleCount, totalCount) {
    const search = String(filters && filters.search || "").trim();
    const status = String(filters && filters.status || "all").trim();
    const action = String(filters && filters.action || "all").trim();
    const side = String(filters && filters.side || "all").trim();
    const parts = [
      `显示 ${visibleCount}/${totalCount}`,
      `状态 ${candidateFilterLabel("status", status)}`,
      `动作 ${candidateFilterLabel("action", action)}`,
      `侧别 ${candidateFilterLabel("side", side)}`,
      `排序 ${candidateSortLabel(filters && filters.sort)}`,
    ];
    if (search) parts.unshift(`搜索 "${search}"`);
    return parts.join(" · ");
  }

  function candidateSummaryCounts(items) {
    const summary = {
      pending: 0,
      approved: 0,
      watched: 0,
      executed: 0,
      ignored: 0,
      rejected: 0,
      expired: 0,
    };
    for (const item of Array.isArray(items) ? items : []) {
      const status = candidateStatusValue(item);
      if (Object.prototype.hasOwnProperty.call(summary, status)) {
        summary[status] += 1;
      } else if (status === "queued") {
        summary.approved += 1;
      }
    }
    return summary;
  }

  function candidateReviewTrail(candidate, side) {
    const status = candidateStatusValue(candidate);
    const action = candidateActionValue(candidate) || String(candidate.suggested_action || "").trim().toLowerCase();
    const reasonText = String(candidate.recommendation_reason || candidate.skip_reason || candidate.note || "").trim();
    const reviewAction = String(candidate.review_action || "").trim().toLowerCase();
    const reviewStatus = String(candidate.review_status || "").trim().toLowerCase();
    const reviewNote = String(candidate.review_note || "").trim();
    const signalTs = Number(candidate.signal_snapshot && candidate.signal_snapshot.timestamp ? Date.parse(candidate.signal_snapshot.timestamp) / 1000 : 0);
    const createdTs = Number(candidate.created_ts || 0);
    const updatedTs = Number(candidate.updated_ts || 0);
    const expiresTs = Number(candidate.expires_ts || 0);
    const trail = [];
    trail.push({
      label: "发现",
      value: `${shortWallet(candidate.wallet)} · ${candidate.trigger_type || "wallet_event"}`,
      detail: `${candidate.source_wallet_count || 1} 个钱包触发 · ${candidate.wallet_tier || "WATCH"}`,
      tone: candidate.source_wallet_count > 1 ? "positive" : "warn",
    });
    trail.push({
      label: "富化",
      value: `${candidate.market_tag || "market"} · ${candidate.resolution_bucket || "unbucketed"}`,
      detail: `${candidate.market_slug || candidate.token_id || "-"} · ${candidate.condition_id || "no condition"}`,
      tone: candidate.market_tag ? "positive" : "neutral",
    });
    trail.push({
      label: "评分",
      value: `score ${Number(candidate.score || 0).toFixed(1)} · wallet ${Number(candidate.wallet_score || 0).toFixed(1)} · conf ${fmtPct(Number(candidate.confidence || 0) * 100, 0)}`,
      detail: `${candidate.source_avg_price ? `来源均价 ${Number(candidate.source_avg_price).toFixed(3)}` : "无来源均价"} · ${candidateActionText(action, side)}`,
      tone: candidate.score >= 75 ? "positive" : candidate.score < 50 ? "danger" : "warn",
    });
    trail.push({
      label: "盘口",
      value: candidateOrderbookSummary(candidate),
      detail: `spread ${candidate.spread_pct == null ? "--" : fmtPct(candidate.spread_pct, 1)} · chase ${candidate.chase_pct == null ? "--" : fmtPct(candidate.chase_pct, 1)}`,
      tone: Number(candidate.chase_pct || 0) > 4 || Number(candidate.spread_pct || 0) > 6 ? "danger" : "neutral",
    });
    trail.push({
      label: "决策",
      value: candidateActionText(candidate.suggested_action, side),
      detail: reasonText || candidate.recommendation_reason || "等待解释",
      tone: candidateExplainTone(candidateExplainTitle(candidate)),
    });
    trail.push({
      label: "复盘",
      value: reviewAction ? candidateActionText(reviewAction, side) : reviewStatus || "waiting",
      detail: `${reviewStatus || "waiting"}${reviewNote ? ` · ${reviewNote}` : ""}`,
      tone: reviewStatus === "executed" ? "positive" : reviewStatus === "ignored" ? "cancel" : reviewStatus === "pending" ? "warn" : "neutral",
    });
    trail.push({
      label: "生命周期",
      value: `${status}${expiresTs > 0 ? ` · ${fmtAge(Math.max(0, expiresTs - Math.floor(Date.now() / 1000)))}` : ""}`,
      detail: `${createdTs > 0 ? `创建 ${fmtDateTime(createdTs)}` : "创建时间未知"}${updatedTs > 0 ? ` · 更新 ${fmtDateTime(updatedTs)}` : ""}${signalTs > 0 ? ` · signal ${fmtDateTime(signalTs)}` : ""}`,
      tone: status === "executed" ? "positive" : status === "rejected" || status === "ignored" ? "danger" : "warn",
    });
    return trail;
  }

  function humanizeReason(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    const labels = {
      pause_opening: "已暂停开仓",
      reduce_only: "当前处于只减仓模式",
      token_add_cooldown: "命中加仓冷却",
      sell_already_processed_this_cycle: "本轮已处理过卖出",
      no_open_position: "当前没有可退出仓位",
      entry_wallet_mismatch: "来源钱包与当前仓位不一致",
      insufficient_budget: "可用预算不足",
      wallet_score_gate: "钱包评分未过跟随阈值",
      position_missing_after_execute: "执行后未能确认仓位",
      market_data_unavailable: "当前没有可交易盘口",
      spread_too_wide: "盘口 spread 过宽",
      chase_too_high: "追价成本过高",
      price_out_of_band: "价格不在允许区间内",
      invalid_notional: "下单金额不满足限制",
      duplicate_signal: "同轮重复信号",
      risk_rejected: "风控拒绝",
      pending_order_exists: "已有进行中的相关订单",
      netting_limited: "同条件暴露已接近上限",
    };
    if (labels[raw]) return labels[raw];
    return raw.replaceAll("_", " ");
  }

  function candidateGateReason(candidate) {
    const skipReason = humanizeReason(candidateField(candidate, "skip_reason") || candidate.skip_reason);
    const riskReason = humanizeReason(candidateField(candidate, "risk_reason") || candidate.risk_reason);
    return skipReason || riskReason || "";
  }

  function candidateGateState(candidate, side) {
    const reason = candidateGateReason(candidate);
    if (!reason) {
      return {
        gated: false,
        label: "已通过门禁",
        detail: "当前可以进入执行层",
        tone: "positive",
        score: Number(candidateField(candidate, "score") || candidate.score || 0),
      };
    }

    const score = Number(candidateField(candidate, "score") || candidate.score || 0);
    const highScore = score >= 70;
    const suggested = String(candidateField(candidate, "suggested_action") || candidate.suggested_action || "").trim().toLowerCase();
    const action = candidateActionText(suggested || candidate.suggested_action, side);
    return {
      gated: true,
      label: highScore ? "高分但被门禁拦住" : "当前不可执行",
      detail: highScore
        ? `综合分 ${score.toFixed(1)} · ${reason}`
        : reason,
      tone: highScore ? "warn" : "danger",
      score,
      action,
      reason,
    };
  }

  function candidateOrderbookSummary(candidate, digits = 3) {
    const bid = candidate.current_best_bid == null ? "--" : Number(candidate.current_best_bid).toFixed(digits);
    const ask = candidate.current_best_ask == null ? "--" : Number(candidate.current_best_ask).toFixed(digits);
    const mid = candidate.current_midpoint == null ? "--" : Number(candidate.current_midpoint).toFixed(digits);
    return `bid ${bid} · ask ${ask} · mid ${mid}`;
  }

  function candidateExplainTone(title) {
    const value = String(title || "").trim();
    if (value === "为什么推荐") return "positive";
    if (value === "为什么先观察") return "warn";
    if (value === "高分但被门禁拦住") return "warn";
    if (value === "当前不可执行") return "danger";
    if (value === "为什么忽略") return "danger";
    return "warn";
  }

  function candidateField(candidate, key) {
    if (!candidate || typeof candidate !== "object") return undefined;
    if (candidate[key] != null && candidate[key] !== "") return candidate[key];
    const nestedKeys = ["payload", "decision_snapshot", "market_context", "topic_snapshot", "control", "risk_snapshot", "netting_snapshot", "price_band"];
    for (const nestedKey of nestedKeys) {
      const nested = candidate[nestedKey];
      if (nested && typeof nested === "object" && nested[key] != null && nested[key] !== "") {
        return nested[key];
      }
    }
    return undefined;
  }

  function candidateExplainTitle(candidate) {
    const status = String(candidateField(candidate, "status") || "").trim().toLowerCase();
    const suggested = String(candidateField(candidate, "suggested_action") || "").trim().toLowerCase();
    const gateState = candidateGateState(candidate, candidateField(candidate, "side") || candidate.side || "BUY");
    if (status === "ignored" || status === "rejected" || suggested === "ignore") return "为什么忽略";
    if (gateState.gated) return gateState.label;
    if (status === "watched" || suggested === "watch") return "为什么先观察";
    return "为什么推荐";
  }

  function candidateExplanation(candidate, side) {
    const suggestedAction = String(candidateField(candidate, "suggested_action") || "").trim();
    const skipReason = humanizeReason(candidateField(candidate, "skip_reason"));
    const riskReason = humanizeReason(candidateField(candidate, "risk_reason"));
    const walletScoreSummary = String(candidateField(candidate, "wallet_score_summary") || "").trim();
    const score = Number(candidateField(candidate, "score") || candidate.score || 0);
    const walletScore = Number(candidateField(candidate, "wallet_score") || candidate.wallet_score || 0);
    const confidence = Number(candidateField(candidate, "confidence") || candidate.confidence || 0);
    const spreadPct = Number(candidateField(candidate, "spread_pct"));
    const chasePct = Number(candidateField(candidate, "chase_pct"));
    const sourceWalletCount = Number(candidateField(candidate, "source_wallet_count") || 0);
    const resonanceHint = String(candidateField(candidate, "candidate_origin") || candidateField(candidate, "trigger_type") || "").toLowerCase();
    const existingPosition = candidateField(candidate, "existing_position") === true;
    const duplicate = candidateField(candidate, "duplicate") === true;
    const nettingLimited = candidateField(candidate, "netting_limited") === true;
    const budgetLimited = candidateField(candidate, "budget_limited") === true;
    const cooldownRemaining = Math.max(0, Number(candidateField(candidate, "cooldown_remaining") || 0), Number(candidateField(candidate, "add_cooldown_remaining") || 0));
    const lines = [];

    if (suggestedAction) {
      lines.push({
        label: skipReason ? "建议处理" : "当前动作",
        value: candidateActionText(suggestedAction, side),
      });
    }
    if (Number.isFinite(score) || Number.isFinite(walletScore) || Number.isFinite(confidence) || sourceWalletCount > 0) {
      lines.push({
        label: "评分拆解",
        value: `score ${score.toFixed(1)} · wallet ${walletScore.toFixed(1)} · conf ${fmtPct(confidence * 100, 0)} · ${sourceWalletCount} wallet${sourceWalletCount === 1 ? "" : "s"}`,
      });
    }
    if (skipReason || riskReason) {
      lines.push({
        label: "忽略 / 风控",
        value: skipReason || riskReason,
      });
    }
    if (Number.isFinite(spreadPct) || Number.isFinite(chasePct)) {
      const parts = [];
      if (Number.isFinite(spreadPct) && spreadPct > 0) parts.push(`spread ${fmtPct(spreadPct, 1)}`);
      if (Number.isFinite(chasePct) && chasePct > 0) parts.push(`追价 ${fmtPct(chasePct, 1)}`);
      if (parts.length > 0) {
        lines.push({
          label: "动量 / 盘口",
          value: parts.join(" · "),
        });
      }
    }
    if (sourceWalletCount > 1 || resonanceHint.includes("resonance") || walletScoreSummary.toLowerCase().includes("resonance")) {
      lines.push({
        label: "共振信号",
        value: sourceWalletCount > 1 ? `${sourceWalletCount} 个钱包同向` : (walletScoreSummary || "多钱包共振"),
      });
    }
    const conflictParts = [];
    if (existingPosition) conflictParts.push("已有仓位");
    if (duplicate) conflictParts.push("同轮重复");
    if (nettingLimited) conflictParts.push("同条件暴露受限");
    if (budgetLimited) conflictParts.push("预算受限");
    if (cooldownRemaining > 0) conflictParts.push(`冷却剩余 ${fmtAge(cooldownRemaining)}`);
    if (conflictParts.length > 0) {
      lines.push({
        label: "风险冲突",
        value: conflictParts.join(" · "),
      });
    }
    if (walletScoreSummary) {
      lines.push({
        label: "钱包画像",
        value: walletScoreSummary,
      });
    }
    if (lines.length <= 0) {
      lines.push({
        label: "说明",
        value: "当前候选没有额外的拦截或共振说明，优先看 suggested_action 与钱包评分。",
      });
    }
    return lines.slice(0, 5);
  }

  function candidateCardReason(candidate, side) {
    const gateState = candidateGateState(candidate, side);
    if (gateState.gated) {
      return gateState.label;
    }
    const primary = [
      String(candidate.recommendation_reason || "").trim(),
      String(candidate.note || "").trim(),
      candidateGateReason(candidate),
    ].find(Boolean);
    if (primary) return primary;
    const explanation = candidateExplanation(candidate, side);
    const line = explanation.find((item) => {
      const label = String(item && item.label || "").trim();
      return item && item.value && !["评分拆解", "当前动作", "建议处理"].includes(label);
    });
    if (line && line.value) return String(line.value).trim();
    const sourceWalletCount = Math.max(1, Number(candidate.source_wallet_count || 1));
    return `${candidateActionText(candidate.suggested_action, side)} · ${sourceWalletCount} 个钱包触发`;
  }

  function candidateCardPriorityItems(candidate, side) {
    const score = Number(candidate.score || 0);
    const walletScore = Number(candidate.wallet_score || 0);
    const observedNotional = Number(candidate.observed_notional || 0);
    const observedSize = Number(candidate.observed_size || 0);
    const qualityParts = [`score ${score.toFixed(0)}`];
    if (walletScore > 0) qualityParts.push(`wallet ${walletScore.toFixed(0)}`);
    const sizeValue = observedNotional > 0
      ? fmtUsd(observedNotional, false)
      : observedSize > 0
        ? `${observedSize.toFixed(0)} 份`
        : "--";
    return [
      {
        label: "综合分",
        value: qualityParts.join(" · "),
      },
      {
        label: "规模",
        value: sizeValue,
      },
    ];
  }

  function candidateCardSummaryLead(candidate, side) {
    const title = candidateExplainTitle(candidate);
    const gateReason = candidateGateReason(candidate);
    const prefix = title === "为什么忽略"
      ? "不看"
      : title === "为什么先观察"
        ? "先看"
        : title === "高分但被门禁拦住"
          ? "高分但被门禁拦住"
          : title === "当前不可执行"
            ? "当前不可执行"
            : "看";
    const action = candidateActionText(candidateActionValue(candidate) || candidate.suggested_action, side);
    const score = Number(candidate.score || 0);
    const walletScore = Number(candidate.wallet_score || 0);
    const confidence = Number(candidate.confidence || 0);
    const sourceWalletCount = Math.max(1, Number(candidate.source_wallet_count || 1));
    const reason = title === "高分但被门禁拦住" || title === "当前不可执行"
      ? gateReason || candidateCardReason(candidate, side)
      : candidateCardReason(candidate, side);
    const pieces = [];
    if (title === "为什么忽略") {
      const skip = gateReason;
      const spread = Number(candidate.spread_pct);
      const chase = Number(candidate.chase_pct);
      if (skip) pieces.push(skip);
      if (!skip && Number.isFinite(spread)) pieces.push(`spread ${fmtPct(spread, 1)}`);
      if (Number.isFinite(chase)) pieces.push(`chase ${fmtPct(chase, 1)}`);
      if (score > 0) pieces.push(`score ${score.toFixed(0)}`);
    } else if (title === "高分但被门禁拦住" || title === "当前不可执行") {
      pieces.push(`score ${score.toFixed(0)}`);
      const gateReason = candidateGateReason(candidate);
      if (gateReason) pieces.push(gateReason);
      if (walletScore > 0) pieces.push(`wallet ${walletScore.toFixed(0)}`);
    } else if (title === "为什么先观察") {
      pieces.push(action);
      if (sourceWalletCount > 1) pieces.push(`${sourceWalletCount} 钱包`);
      if (walletScore > 0) pieces.push(`wallet ${walletScore.toFixed(0)}`);
      if (confidence > 0) pieces.push(`conf ${fmtPct(confidence * 100, 0)}`);
    } else {
      pieces.push(action);
      if (score > 0) pieces.push(`score ${score.toFixed(0)}`);
      if (sourceWalletCount > 1) pieces.push(`${sourceWalletCount} 钱包`);
      if (walletScore > 0) pieces.push(`wallet ${walletScore.toFixed(0)}`);
    }
    const tail = pieces.filter(Boolean).join(" · ");
    const combined = `${prefix}：${reason}${tail ? ` · ${tail}` : ""}`;
    return combined.length > 72 ? `${combined.slice(0, 71)}…` : combined;
  }

  function candidateCardHoverText(candidate, side, reason, priorityItems) {
    const sourceWalletCount = Math.max(1, Number(candidate.source_wallet_count || 1));
    const bid = candidate.current_best_bid == null ? "--" : Number(candidate.current_best_bid).toFixed(3);
    const ask = candidate.current_best_ask == null ? "--" : Number(candidate.current_best_ask).toFixed(3);
    const mid = candidate.current_midpoint == null ? "--" : Number(candidate.current_midpoint).toFixed(3);
    const spread = candidate.spread_pct == null ? "--" : fmtPct(candidate.spread_pct, 1);
    const chase = candidate.chase_pct == null ? "--" : fmtPct(candidate.chase_pct, 1);
    const wallet = shortWallet(candidate.wallet);
    const action = candidateActionText(candidateActionValue(candidate) || candidate.suggested_action, side);
    const gateState = candidateGateState(candidate, side);
    const lines = [
      `${candidate.market_slug || "-"} · ${candidate.outcome || "--"} · ${side}`,
      `${wallet}${candidate.wallet_tier ? ` · ${candidate.wallet_tier}` : ""} · ${sourceWalletCount} 钱包`,
      reason,
      gateState.gated ? `${gateState.label} · ${gateState.reason}` : "门禁通过",
      `score ${Number(candidate.score || 0).toFixed(1)} · wallet ${Number(candidate.wallet_score || 0).toFixed(1)} · conf ${fmtPct(Number(candidate.confidence || 0) * 100, 0)}`,
      `bid ${bid} · ask ${ask} · mid ${mid} · spread ${spread} · chase ${chase}`,
      `${action} · ${candidateCardReason(candidate, side)}`,
    ];
    for (const item of Array.isArray(priorityItems) ? priorityItems : []) {
      if (!item || !item.label || !item.value) continue;
      lines.push(`${item.label}: ${item.value}`);
    }
    return lines.join("\n");
  }

  function candidateCardShortSummary(candidate, side) {
    return candidateCardSummaryLead(candidate, side);
  }

  function candidateFactorTone(value, positiveThreshold = 0.75, dangerThreshold = 0.4) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return "neutral";
    if (numeric >= positiveThreshold) return "positive";
    if (numeric <= dangerThreshold) return "danger";
    return "warn";
  }

  function candidateFactorValueClass(tone) {
    const value = String(tone || "").trim().toLowerCase();
    if (value === "positive") return "value-positive";
    if (value === "danger") return "value-negative";
    return "value-neutral";
  }

  function normalizeFactorTone(value) {
    const tone = String(value || "").trim().toLowerCase();
    if (tone === "bullish" || tone === "positive" || tone === "ok") return "positive";
    if (tone === "bearish" || tone === "danger" || tone === "negative") return "danger";
    if (tone === "warn" || tone === "warning") return "warn";
    return "neutral";
  }

  function factorPillTone(tone) {
    const value = normalizeFactorTone(tone);
    if (value === "positive") return "bullish";
    if (value === "danger") return "bearish";
    return "neutral";
  }

  function factorPillText(tone) {
    const value = normalizeFactorTone(tone);
    if (value === "positive") return "偏正";
    if (value === "danger") return "偏负";
    if (value === "warn") return "注意";
    return "中性";
  }

  function candidateReasonFactors(candidate) {
    const rows = Array.isArray(candidate && candidate.reason_factors) ? candidate.reason_factors : [];
    return rows
      .filter((item) => item && typeof item === "object")
      .map((item) => {
        const key = String(item.key || item.label || "factor").trim();
        const label = String(item.label || key || "factor").trim();
        const value = item.value == null || String(item.value).trim() === "" ? "--" : String(item.value);
        const detail = String(item.detail || "").trim();
        const rawDirection = String(item.direction || "").trim().toLowerCase();
        let weight = Number(item.weight);
        if (!Number.isFinite(weight)) weight = null;
        return {
          key,
          label,
          value,
          detail,
          rawDirection: rawDirection || "neutral",
          tone: normalizeFactorTone(rawDirection),
          weight,
        };
      });
  }

  function candidateFactorCards(candidate, side) {
    const score = Number(candidateField(candidate, "score") || candidate.score || 0);
    const walletScore = Number(candidateField(candidate, "wallet_score") || candidate.wallet_score || 0);
    const confidence = Number(candidateField(candidate, "confidence") || candidate.confidence || 0);
    const sourceWalletCount = Number(candidateField(candidate, "source_wallet_count") || candidate.source_wallet_count || 0);
    const momentum5m = Number(candidateField(candidate, "momentum_5m") || candidate.momentum_5m || 0);
    const momentum30m = Number(candidateField(candidate, "momentum_30m") || candidate.momentum_30m || 0);
    const spreadPct = candidateField(candidate, "spread_pct");
    const chasePct = candidateField(candidate, "chase_pct");
    const triggerType = String(candidateField(candidate, "trigger_type") || candidate.trigger_type || "wallet_event").trim();
    const walletTier = String(candidateField(candidate, "wallet_tier") || candidate.wallet_tier || "WATCH").trim();
    const recommendationReason = String(candidateField(candidate, "recommendation_reason") || candidate.recommendation_reason || candidate.skip_reason || "").trim();
    const currentBid = candidate.current_best_bid == null ? null : Number(candidate.current_best_bid);
    const currentAsk = candidate.current_best_ask == null ? null : Number(candidate.current_best_ask);
    const currentMid = candidate.current_midpoint == null ? null : Number(candidate.current_midpoint);
    const existingPosition = candidateField(candidate, "existing_position") === true
      || candidate.existing_position_conflict === true
      || candidate.has_existing_position === true;
    const existingNotional = Number(candidateField(candidate, "existing_position_notional") || candidate.existing_position_notional || 0);
    const skipReason = humanizeReason(candidateField(candidate, "skip_reason") || candidate.skip_reason);
    const riskReason = humanizeReason(candidateField(candidate, "risk_reason") || candidate.risk_reason);
    const duplicate = candidateField(candidate, "duplicate") === true;
    const nettingLimited = candidateField(candidate, "netting_limited") === true;
    const budgetLimited = candidateField(candidate, "budget_limited") === true;
    const cooldownRemaining = Math.max(0, Number(candidateField(candidate, "cooldown_remaining") || candidate.cooldown_remaining || 0), Number(candidateField(candidate, "add_cooldown_remaining") || candidate.add_cooldown_remaining || 0));
    const spreadValue = Number(spreadPct);
    const chaseValue = Number(chasePct);
    const costPressure = Number.isFinite(spreadValue) || Number.isFinite(chaseValue)
      ? (Math.max(0, Number.isFinite(spreadValue) ? spreadValue : 0) + Math.max(0, Number.isFinite(chaseValue) ? chaseValue : 0))
      : null;
    const momentumSummary = `${fmtSignedRatioPct(momentum5m, 1)} / ${fmtSignedRatioPct(momentum30m, 1)}`;
    const explainTitle = String(candidateExplainTitle(candidate));
    const recommendedTone = candidateExplainTone(explainTitle);
    const gateState = candidateGateState(candidate, side);
    const reasonFactors = candidateReasonFactors(candidate);

    const fallbackBreakdown = [
      {
        label: "执行门禁",
        value: gateState.gated ? gateState.label : "已通过",
        detail: gateState.gated ? gateState.detail : "当前可以进入执行层",
        tone: gateState.tone,
      },
      {
        label: "综合评分",
        value: score.toFixed(1),
        detail: recommendationReason || candidateActionText(candidate.suggested_action, side),
        tone: candidateFactorTone(score / 100, 0.9, 0.7),
      },
      {
        label: "钱包评分",
        value: `${walletScore.toFixed(1)}${walletTier ? ` · ${walletTier}` : ""}`,
        detail: String(candidate.wallet_tag || candidate.wallet_score_summary || "钱包质量"),
        tone: candidateFactorTone(walletScore / 100, 0.85, 0.6),
      },
      {
        label: "置信度",
        value: fmtPct(confidence * 100, 0),
        detail: String(candidate.wallet_score_summary || recommendationReason || "决策置信度"),
        tone: candidateFactorTone(confidence, 0.7, 0.45),
      },
      {
        label: "共振钱包",
        value: `${sourceWalletCount} wallet${sourceWalletCount === 1 ? "" : "s"}`,
        detail: sourceWalletCount > 1 ? "multi-wallet confirm" : "single wallet trigger",
        tone: sourceWalletCount > 1 ? "positive" : "warn",
      },
      {
        label: "动量",
        value: momentumSummary,
        detail: momentum30m < 0 ? "30m 回撤" : "顺风动量",
        tone: momentum30m > 0 ? "positive" : momentum30m < -10 ? "danger" : "warn",
      },
      {
        label: "盘口成本",
        value: `${spreadPct == null || spreadPct === "" ? "--" : fmtPct(spreadValue, 1)} / ${chasePct == null || chasePct === "" ? "--" : fmtPct(chaseValue, 1)}`,
        detail: `bid ${currentBid == null ? "--" : currentBid.toFixed(3)} · ask ${currentAsk == null ? "--" : currentAsk.toFixed(3)} · mid ${currentMid == null ? "--" : currentMid.toFixed(3)}`,
        tone: costPressure == null ? "warn" : costPressure > 6 ? "danger" : costPressure > 2 ? "warn" : "positive",
      },
      {
        label: "触发类型",
        value: triggerType || "wallet_event",
        detail: String(candidate.market_tag || candidate.resolution_bucket || "market context"),
        tone: recommendedTone,
      },
    ];

    const fallbackConflicts = [];
    if (existingPosition) {
      fallbackConflicts.push({
        label: "已有仓位",
        value: existingNotional > 0 ? fmtUsd(existingNotional, false) : "position conflict",
        detail: "同 token / 同条件已有暴露",
        tone: "danger",
      });
    }
    if (duplicate) {
      fallbackConflicts.push({
        label: "重复信号",
        value: "同轮已去重",
        detail: "避免同一候选重复执行",
        tone: "warn",
      });
    }
    if (nettingLimited) {
      fallbackConflicts.push({
        label: "净额上限",
        value: "netting capped",
        detail: "同条件暴露已接近上限",
        tone: "warn",
      });
    }
    if (budgetLimited) {
      fallbackConflicts.push({
        label: "预算受限",
        value: "budget capped",
        detail: "当前可用名义金额不足",
        tone: "danger",
      });
    }
    if (cooldownRemaining > 0) {
      fallbackConflicts.push({
        label: "冷却中",
        value: fmtAge(cooldownRemaining),
        detail: "加仓 / 跟随冷却仍未结束",
        tone: "warn",
      });
    }
    if (riskReason) {
      fallbackConflicts.push({
        label: "风控原因",
        value: riskReason,
        detail: skipReason || "risk gate",
        tone: "danger",
      });
    } else if (skipReason) {
      fallbackConflicts.push({
        label: "忽略原因",
        value: skipReason,
        detail: "skip / gate / cooldown",
        tone: "warn",
      });
    }
    if (fallbackConflicts.length <= 0) {
      fallbackConflicts.push({
        label: "风险冲突",
        value: "none",
        detail: "没有显著的已有仓位 / 重复 / 冷却 / 预算冲突",
        tone: "positive",
      });
    }

    const conflictKeys = new Set(["existing_position", "skip_reason"]);
    const factorBreakdown = reasonFactors
      .filter((item) => !conflictKeys.has(String(item.key || "").trim().toLowerCase()) && String(item.key || "").trim().toLowerCase() !== "decision")
      .map((item) => ({
        ...item,
        detail: item.detail || item.label,
      }));
    const factorConflicts = reasonFactors
      .filter((item) => conflictKeys.has(String(item.key || "").trim().toLowerCase()))
      .map((item) => ({
        ...item,
        detail: item.detail || item.label,
      }));

    const breakdown = factorBreakdown.length > 0 ? factorBreakdown.slice(0, 6) : fallbackBreakdown;
    const conflicts = factorConflicts.length > 0 ? factorConflicts.slice(0, 4) : fallbackConflicts;

    return { breakdown, conflicts };
  }

  function renderFactorCards(items) {
    const rows = Array.isArray(items) ? items : [];
    return rows.length > 0
      ? rows.map((item) => {
          const tone = normalizeFactorTone(item && item.tone || "neutral");
          const valueClass = candidateFactorValueClass(tone);
          const rawWeight = Number(item && item.weight);
          const weightText = Number.isFinite(rawWeight) && Math.abs(rawWeight) > 0.05 ? `${rawWeight >= 0 ? "+" : ""}${rawWeight.toFixed(1)}` : "";
          const pillTone = factorPillTone(item && item.rawDirection ? item.rawDirection : tone);
          const pillText = factorPillText(item && item.rawDirection ? item.rawDirection : tone);
          return `<div class="component-card candidate-factor-card factor-card factor-${tone || "neutral"}">
            <div class="factor-head">
              <span>${escapeHtml(String(item && item.label || "factor"))}</span>
              <span class="factor-pill factor-${pillTone}">${pillText}</span>
            </div>
            <b class="${valueClass}">${escapeHtml(String(item && item.value || "--"))}</b>
            ${weightText ? `<small class="factor-weight">权重 ${escapeHtml(weightText)}</small>` : ""}
            <p class="factor-detail">${escapeHtml(String(item && item.detail || ""))}</p>
          </div>`;
        }).join("")
      : '<div class="component-card factor-card factor-neutral"><span>factor</span><b>等待数据...</b><p>暂无可展示的因子</p></div>';
  }

  function renderCandidateSummaryCards(items) {
    const rows = Array.isArray(items) ? items.filter((item) => item && typeof item === "object") : [];
    if (rows.length <= 0) {
      return '<div class="component-card candidate-summary-card factor-card factor-neutral"><span>决策摘要</span><b>等待数据...</b><p>暂无可展示的摘要因子</p></div>';
    }

    const buildCard = (item, featured = false) => {
      const tone = normalizeFactorTone(item && item.tone || "neutral");
      const valueClass = candidateFactorValueClass(tone);
      const rawWeight = Number(item && item.weight);
      const weightText = Number.isFinite(rawWeight) && Math.abs(rawWeight) > 0.05 ? `${rawWeight >= 0 ? "+" : ""}${rawWeight.toFixed(1)}` : "";
      const pillTone = factorPillTone(item && item.rawDirection ? item.rawDirection : tone);
      const pillText = factorPillText(item && item.rawDirection ? item.rawDirection : tone);
      const label = escapeHtml(String(item && item.label || "factor"));
      const value = escapeHtml(String(item && item.value || "--"));
      const detail = escapeHtml(String(item && item.detail || item && item.label || ""));
      if (featured) {
        return `<article class="component-card candidate-summary-hero factor-card factor-${tone || "neutral"}">
          <div class="candidate-summary-hero-head">
            <div class="candidate-summary-hero-copy">
              <span class="candidate-summary-eyebrow">${label}</span>
              <b class="${valueClass}">${value}</b>
            </div>
            <span class="factor-pill factor-${pillTone}">${pillText}</span>
          </div>
          ${weightText ? `<small class="factor-weight">权重 ${escapeHtml(weightText)}</small>` : ""}
          <p class="candidate-summary-detail">${detail}</p>
        </article>`;
      }
      return `<article class="component-card candidate-summary-card factor-card factor-${tone || "neutral"}">
        <div class="candidate-summary-card-head">
          <span>${label}</span>
          <span class="factor-pill factor-${pillTone}">${pillText}</span>
        </div>
        <b class="${valueClass}">${value}</b>
        ${weightText ? `<small class="factor-weight">权重 ${escapeHtml(weightText)}</small>` : ""}
        <p>${detail}</p>
      </article>`;
    };

    const [hero, ...rest] = rows;
    return [
      buildCard(hero, true),
      rest.length > 0
        ? `<div class="candidate-summary-grid">${rest.slice(0, 4).map((item) => buildCard(item)).join("")}</div>`
        : "",
    ].join("");
  }

  function candidateFocusStrip(candidate, side) {
    const reasonFactors = candidateReasonFactors(candidate);
    const factorCards = candidateFactorCards(candidate, side);
    const decisionFactor = reasonFactors.find((item) => String(item.key || "").trim().toLowerCase() === "decision");
    const topReasons = reasonFactors
      .filter((item) => String(item.key || "").trim().toLowerCase() !== "decision")
      .sort((left, right) => Math.abs(Number(right.weight || 0)) - Math.abs(Number(left.weight || 0)))
      .slice(0, 3);
    const scoreCard = factorCards.breakdown.find((item) => String(item.label || "") === "综合评分") || factorCards.breakdown[0] || {};
    const momentumCard = factorCards.breakdown.find((item) => String(item.label || "") === "动量" || String(item.label || "") === "价格动量") || factorCards.breakdown[4] || {};
    const costCard = factorCards.breakdown.find((item) => String(item.label || "") === "盘口成本" || String(item.label || "") === "盘口点差" || String(item.label || "") === "追价幅度") || factorCards.breakdown[5] || {};
    const riskCard = factorCards.conflicts[0] || {};
    const actionText = candidateActionText(candidate.suggested_action, side);
    const gateState = candidateGateState(candidate, side);
    const strip = [
      {
        label: "执行门禁",
        value: gateState.gated ? gateState.label : "已通过",
        detail: gateState.gated ? gateState.detail : "当前可以进入执行层",
        tone: gateState.tone,
      },
      {
        label: "决策主张",
        value: actionText,
        detail: String(decisionFactor && decisionFactor.detail || candidate.recommendation_reason || candidate.skip_reason || candidate.note || candidate.wallet_score_summary || "当前候选摘要"),
        tone: normalizeFactorTone(decisionFactor && decisionFactor.tone || candidateExplainTone(candidateExplainTitle(candidate))),
        rawDirection: decisionFactor && decisionFactor.rawDirection ? decisionFactor.rawDirection : "",
        weight: decisionFactor && Number.isFinite(Number(decisionFactor.weight)) ? Number(decisionFactor.weight) : null,
      },
      {
        label: "综合评分",
        value: String(scoreCard.value || "0.0"),
        detail: String(scoreCard.detail || "score breakdown"),
        tone: String(scoreCard.tone || "neutral"),
      },
      {
        label: "动量",
        value: String(momentumCard.value || "--"),
        detail: String(momentumCard.detail || "momentum"),
        tone: String(momentumCard.tone || "neutral"),
      },
      {
        label: "盘口",
        value: String(costCard.value || "--"),
        detail: String(costCard.detail || "spread / chase"),
        tone: String(costCard.tone || "neutral"),
      },
      {
        label: "风险",
        value: String(riskCard.value || "none"),
        detail: String(riskCard.detail || "no conflict"),
        tone: String(riskCard.tone || "neutral"),
      },
    ];
    for (const item of topReasons) {
      const exists = strip.some((row) => String(row.label || "") === String(item.label || ""));
      if (!exists) {
        strip.push({
          label: item.label,
          value: item.value,
          detail: item.detail || item.label,
          tone: item.tone,
          rawDirection: item.rawDirection,
          weight: item.weight,
        });
      }
    }
    return strip.slice(0, 5);
  }

  function candidateReviewTrail(candidate, side) {
    const status = candidateStatusValue(candidate);
    const action = candidateActionValue(candidate) || String(candidate.suggested_action || "").trim().toLowerCase();
    const reasonText = String(candidate.recommendation_reason || candidate.skip_reason || candidate.note || "").trim();
    const reviewAction = String(candidate.review_action || "").trim().toLowerCase();
    const reviewStatus = String(candidate.review_status || "").trim().toLowerCase();
    const reviewNote = String(candidate.review_note || "").trim();
    const createdTs = Number(candidate.created_ts || 0);
    const updatedTs = Number(candidate.updated_ts || 0);
    const expiresTs = Number(candidate.expires_ts || 0);
    const signalTsRaw = candidate.signal_snapshot && candidate.signal_snapshot.timestamp ? Date.parse(candidate.signal_snapshot.timestamp) : 0;
    const signalTs = Number.isFinite(signalTsRaw) ? Math.floor(signalTsRaw / 1000) : 0;
    return [
      {
        label: "发现",
        value: `${shortWallet(candidate.wallet)} · ${candidate.trigger_type || "wallet_event"}`,
        detail: `${candidate.source_wallet_count || 1} 个钱包触发 · ${candidate.wallet_tier || "WATCH"}`,
        tone: candidate.source_wallet_count > 1 ? "positive" : "warn",
      },
      {
        label: "富化",
        value: `${candidate.market_tag || "market"} · ${candidate.resolution_bucket || "unbucketed"}`,
        detail: `${candidate.market_slug || candidate.token_id || "-"} · ${candidate.condition_id || "no condition"}`,
        tone: candidate.market_tag ? "positive" : "neutral",
      },
      {
        label: "评分",
        value: `score ${Number(candidate.score || 0).toFixed(1)} · wallet ${Number(candidate.wallet_score || 0).toFixed(1)} · conf ${fmtPct(Number(candidate.confidence || 0) * 100, 0)}`,
        detail: `${candidate.source_avg_price ? `来源均价 ${Number(candidate.source_avg_price).toFixed(3)}` : "无来源均价"} · ${candidateActionText(action, side)}`,
        tone: candidate.score >= 75 ? "positive" : candidate.score < 50 ? "danger" : "warn",
      },
      {
        label: "盘口",
        value: candidateOrderbookSummary(candidate),
        detail: `spread ${candidate.spread_pct == null ? "--" : fmtPct(candidate.spread_pct, 1)} · chase ${candidate.chase_pct == null ? "--" : fmtPct(candidate.chase_pct, 1)}`,
        tone: Number(candidate.chase_pct || 0) > 4 || Number(candidate.spread_pct || 0) > 6 ? "danger" : "neutral",
      },
      {
        label: "决策",
        value: candidateActionText(candidate.suggested_action, side),
        detail: reasonText || candidate.recommendation_reason || "等待解释",
        tone: candidateExplainTone(candidateExplainTitle(candidate)),
      },
      {
        label: "复盘",
        value: reviewAction ? candidateActionText(reviewAction, side) : reviewStatus || "waiting",
        detail: `${reviewStatus || "waiting"}${reviewNote ? ` · ${reviewNote}` : ""}`,
        tone: reviewStatus === "executed" ? "positive" : reviewStatus === "ignored" ? "cancel" : reviewStatus === "pending" ? "warn" : "neutral",
      },
      {
        label: "生命周期",
        value: `${status}${expiresTs > 0 ? ` · ${fmtAge(Math.max(0, expiresTs - Math.floor(Date.now() / 1000)))}` : ""}`,
        detail: `${createdTs > 0 ? `创建 ${fmtDateTime(createdTs)}` : "创建时间未知"}${updatedTs > 0 ? ` · 更新 ${fmtDateTime(updatedTs)}` : ""}${signalTs > 0 ? ` · signal ${fmtDateTime(signalTs)}` : ""}`,
        tone: status === "executed" ? "positive" : status === "rejected" || status === "ignored" ? "danger" : "warn",
      },
    ];
  }

  function candidateObjectRows(payload, limit = 5) {
    if (!payload || typeof payload !== "object") return [];
    return Object.entries(payload)
      .filter(([, value]) => value != null && String(value).trim() !== "")
      .slice(0, limit)
      .map(([key, value]) => ({
        key: String(key).replaceAll("_", " "),
        value: Array.isArray(value)
          ? value.join(" / ")
          : typeof value === "object"
            ? JSON.stringify(value)
            : String(value),
      }));
  }

  function renderCandidateDetailPanel(candidate, side) {
    const metaEl = $("candidate-detail-meta");
    const overviewEl = $("candidate-detail-overview");
    const timelineEl = $("candidate-detail-timeline");
    const chainEl = $("candidate-detail-chain");
    const actionsEl = $("candidate-detail-actions");
    const journalEl = $("candidate-detail-journal");
    if (!metaEl || !overviewEl || !timelineEl || !chainEl || !actionsEl || !journalEl) return;

    const emptyLists = () => {
      overviewEl.innerHTML = '<div class="component-card"><span>详情页</span><b>等待数据...</b></div>';
      timelineEl.innerHTML = '<li><div class="review-main"><span>时间轴</span><b>等待数据...</b></div></li>';
      chainEl.innerHTML = '<li><div class="review-main"><span>决策链</span><b>等待数据...</b></div></li>';
      actionsEl.innerHTML = '<li><div class="review-main"><span>动作</span><b>等待数据...</b></div></li>';
      journalEl.innerHTML = '<li><div class="review-main"><span>日记</span><b>等待数据...</b></div></li>';
    };

    if (!candidate || typeof candidate !== "object") {
      metaEl.textContent = "等待候选详情...";
      emptyLists();
      return;
    }

    const candidateId = candidateKey(candidate);
    const detailReady = candidateDetailMatches(candidate, lastCandidateDetail);
    const pending = lastCandidateDetailApiState.pending && String(lastCandidateDetailApiState.candidateId || "") === candidateId;
    if (!detailReady && pending) {
      metaEl.textContent = `${candidateId || "candidate"} · 正在加载详情`;
      overviewEl.innerHTML = '<div class="component-card"><span>详情页</span><b>正在加载...</b><p>正在读取单候选复盘链路、动作和日记。</p></div>';
      timelineEl.innerHTML = '<li><div class="review-main"><span>时间轴</span><b>正在加载...</b></div></li>';
      chainEl.innerHTML = '<li><div class="review-main"><span>决策链</span><b>正在加载...</b></div></li>';
      actionsEl.innerHTML = '<li><div class="review-main"><span>动作</span><b>正在加载...</b></div></li>';
      journalEl.innerHTML = '<li><div class="review-main"><span>日记</span><b>正在加载...</b></div></li>';
      return;
    }

    if (!detailReady && !lastCandidateDetailApiState.ok && String(lastCandidateDetailApiState.candidateId || "") === candidateId) {
      metaEl.textContent = `${candidateId || "candidate"} · 详情不可用`;
      overviewEl.innerHTML = `<div class="component-card"><span>详情页</span><b>加载失败</b><p>${escapeHtml(lastCandidateDetailApiState.error || "candidate detail unavailable")}</p></div>`;
      timelineEl.innerHTML = '<li><div class="review-main"><span>时间轴</span><b>不可用</b></div></li>';
      chainEl.innerHTML = '<li><div class="review-main"><span>决策链</span><b>不可用</b></div></li>';
      actionsEl.innerHTML = '<li><div class="review-main"><span>动作</span><b>不可用</b></div></li>';
      journalEl.innerHTML = '<li><div class="review-main"><span>日记</span><b>不可用</b></div></li>';
      return;
    }

    const detail = detailReady ? lastCandidateDetail : EMPTY_CANDIDATE_DETAIL;
    const trace = detail.trace && typeof detail.trace === "object" ? detail.trace : {};
    const summary = detail.summary && typeof detail.summary === "object" ? detail.summary : EMPTY_CANDIDATE_DETAIL.summary;
    const timeline = Array.isArray(detail.timeline) ? detail.timeline.slice(0, 12) : [];
    const decisionChain = Array.isArray(detail.decision_chain) ? detail.decision_chain.slice(0, 10) : [];
    const relatedActions = Array.isArray(detail.related_actions) ? detail.related_actions.slice(0, 10) : [];
    const relatedJournal = Array.isArray(detail.related_journal) ? detail.related_journal.slice(0, 10) : [];
    const orders = Array.isArray(detail.orders) ? detail.orders : [];
    const traceStatus = String(trace.status || candidate.status || "pending").trim() || "pending";
    const latestTs = Math.max(
      0,
      ...timeline.map((item) => Number(item && item.ts || 0)),
      Number(candidate.updated_ts || candidate.created_ts || 0)
    );
    const traceOpenedTs = Number(trace.opened_ts || trace.ts || 0);
    const gateState = candidateGateState(candidate, side);

    metaEl.textContent = `${candidate.market_slug || candidate.token_id || "candidate"} · ${timeline.length} timeline / ${decisionChain.length} chain`;
    overviewEl.innerHTML = [
      `<div class="component-card candidate-gate-card candidate-gate-${gateState.tone}"><span>执行门禁</span><b>${escapeHtml(gateState.label)}</b><p>${escapeHtml(gateState.reason || gateState.detail || "当前可以进入执行层")}${gateState.gated && Number(candidate.score || 0) > 0 ? ` · score ${Number(candidate.score || 0).toFixed(1)}` : ""}</p></div>`,
      `<div class="component-card"><span>Trace</span><b>${escapeHtml(String(trace.trace_id || candidate.trace_id || "--"))}</b><p>${traceOpenedTs > 0 ? `opened ${fmtDateTime(traceOpenedTs)}` : "未记录 trace opened"}</p></div>`,
      `<div class="component-card"><span>链路状态</span><b class="${traceStatus === "open" || traceStatus === "approved" ? "value-positive" : traceStatus === "closed" || traceStatus === "executed" ? "value-neutral" : "value-negative"}">${escapeHtml(traceStatus)}</b><p>orders ${Number(summary.order_count || orders.length || 0)} · actions ${Number(summary.related_action_count || relatedActions.length || 0)}</p></div>`,
      `<div class="component-card"><span>决策链</span><b>${Number(summary.decision_chain_count || decisionChain.length || 0)} steps</b><p>cycles ${Number(summary.cycle_count || 0)} · trace ${summary.trace_found ? "found" : "missing"}</p></div>`,
      `<div class="component-card"><span>最近事件</span><b>${latestTs > 0 ? fmtDateTime(latestTs) : "--"}</b><p>journal ${Number(summary.related_journal_count || relatedJournal.length || 0)} · ${candidateActionText(candidate.suggested_action, side)}</p></div>`,
    ].join("");

    timelineEl.innerHTML = timeline.length > 0
      ? timeline.map((item) => {
          const kind = String(item.kind || "event");
          const detailText = [
            item.action_label || item.action || item.result_tag || item.flow || "",
            item.status || "",
            item.trace_id || item.cycle_id || "",
          ].filter(Boolean).join(" · ");
          return `<li>
            <div class="review-main">
              <span><span class="tag wait">${escapeHtml(kind)}</span> ${escapeHtml(String(item.text || "-"))}</span>
              <b>${Number(item.ts || 0) > 0 ? fmtDateTime(item.ts) : "--"}</b>
            </div>
            <div class="review-sub">${escapeHtml(detailText || "无额外事件细节")}</div>
          </li>`;
        }).join("")
      : '<li><div class="review-main"><span>时间轴</span><b>暂无事件</b></div></li>';

    chainEl.innerHTML = decisionChain.length > 0
      ? decisionChain.map((item) => {
          const mainText = [item.action_label || item.action || "-", item.side || "", item.wallet_tier || ""].filter(Boolean).join(" · ");
          const detailText = [
            item.topic_label ? `topic ${item.topic_label}` : "",
            item.order_status ? `order ${item.order_status}` : "",
            item.order_notional ? fmtUsd(item.order_notional, false) : "",
            item.final_status || "",
          ].filter(Boolean).join(" · ");
          return `<li>
            <div class="review-main">
              <span>${escapeHtml(mainText)}</span>
              <b>${escapeHtml(String(item.cycle_id || "--"))}</b>
            </div>
            <div class="review-sub">${escapeHtml(detailText || "无额外决策细节")}${Number(item.ts || 0) > 0 ? ` · ${fmtDateTime(item.ts)}` : ""}</div>
          </li>`;
        }).join("")
      : '<li><div class="review-main"><span>决策链</span><b>暂无链路</b></div></li>';

    actionsEl.innerHTML = relatedActions.length > 0
      ? relatedActions.map((item) => {
          const notional = Number(item.notional || 0);
          return `<li>
            <div class="review-main">
              <span>${escapeHtml(candidateActionText(item.action, side))}</span>
              <b>${Number(item.created_ts || 0) > 0 ? fmtDateTime(item.created_ts) : "--"}</b>
            </div>
            <div class="review-sub">${escapeHtml(String(item.note || "无备注"))}${notional > 0 ? ` · ${fmtUsd(notional, false)}` : ""}${item.status ? ` · ${escapeHtml(String(item.status))}` : ""}</div>
          </li>`;
        }).join("")
      : '<li><div class="review-main"><span>动作</span><b>暂无记录</b></div></li>';

    journalEl.innerHTML = relatedJournal.length > 0
      ? relatedJournal.map((item) => {
          const pnl = Number(item.pnl_realized || 0);
          return `<li>
            <div class="review-main">
              <span>${escapeHtml(candidateActionText(item.action, side))}</span>
              <b>${Number(item.created_ts || 0) > 0 ? fmtDateTime(item.created_ts) : "--"}</b>
            </div>
            <div class="review-sub">${escapeHtml(String(item.rationale || item.text || "无备注"))}${item.result_tag ? ` · ${escapeHtml(String(item.result_tag))}` : ""}${pnl !== 0 ? ` · ${fmtUsd(pnl)}` : ""}</div>
          </li>`;
        }).join("")
      : '<li><div class="review-main"><span>日记</span><b>暂无记录</b></div></li>';
  }

  function renderCandidateFocus(candidatesPayload, apiState = { ok: true, error: "" }) {
    const payload = candidatesPayload && typeof candidatesPayload === "object" ? candidatesPayload : EMPTY_CANDIDATES;
    const items = Array.isArray(payload.items) ? payload.items : [];
    const detailRoot = $("candidate-focus");
    const metaEl = $("candidate-focus-meta");
    const headEl = $("candidate-focus-head");
    const summaryEl = $("candidate-focus-summary");
    const listEl = $("candidate-focus-list");
    const metricEl = $("candidate-focus-metrics");
    const breakdownMetaEl = $("candidate-focus-breakdown-meta");
    const breakdownEl = $("candidate-focus-breakdown");
    const conflictMetaEl = $("candidate-focus-conflict-meta");
    const conflictEl = $("candidate-focus-conflict");
    const factorsMetaEl = $("candidate-focus-factors-meta");
    const factorsEl = $("candidate-focus-factors");
    const trailMetaEl = $("candidate-focus-trail-meta");
    const trailEl = $("candidate-focus-trail");
    const explainMetaEl = $("candidate-focus-explain-meta");
    const explainEl = $("candidate-focus-explain");
    const snapshotEl = $("candidate-focus-snapshot");
    const actionEl = $("candidate-focus-action");
    const statusEl = $("candidate-focus-status");
    if (!detailRoot || !metaEl || !headEl || !summaryEl || !listEl || !metricEl || !breakdownMetaEl || !breakdownEl || !conflictMetaEl || !conflictEl || !factorsMetaEl || !factorsEl || !trailMetaEl || !trailEl || !explainMetaEl || !explainEl || !snapshotEl || !actionEl || !statusEl) return;

    if (!apiState.ok && items.length <= 0) {
      detailRoot.classList.add("panel-error");
      metaEl.textContent = "candidate focus unavailable";
      headEl.innerHTML = '<span class="tag danger">错误</span><span class="mono">无法加载候选详情</span>';
      summaryEl.textContent = String(apiState.error || "candidate detail unavailable");
      listEl.innerHTML = '<li><span>状态</span><b>接口暂不可用</b></li>';
      metricEl.innerHTML = '<div class="component-card"><span>候选深读</span><b>接口不可用</b></div>';
      breakdownMetaEl.textContent = "0 factors";
      breakdownEl.innerHTML = '<div class="component-card factor-card factor-danger"><span>score breakdown</span><b>接口不可用</b><p>候选因子拆解暂时无法加载</p></div>';
      conflictMetaEl.textContent = "0 conflicts";
      conflictEl.innerHTML = '<div class="component-card factor-card factor-danger"><span>风险冲突</span><b>接口不可用</b><p>风险冲突拆解暂时无法加载</p></div>';
      factorsMetaEl.textContent = "0 factors";
      factorsEl.innerHTML = '<div class="component-card candidate-summary-card factor-card factor-danger"><span>决策摘要</span><b>接口不可用</b><p>无法加载候选总览因子</p></div>';
      trailMetaEl.textContent = "0 steps";
      trailEl.innerHTML = '<li><div class="review-main"><span>复盘链路</span><b>接口不可用</b></div></li>';
      explainMetaEl.textContent = "0 lines";
      explainEl.innerHTML = '<li><div class="review-main"><span>说明</span><b>等待数据...</b></div></li>';
      snapshotEl.innerHTML = '<div class="component-card"><span>snapshot</span><b>等待数据...</b></div>';
      actionEl.innerHTML = '<li><div class="review-main"><span>补充记录</span><b>不可用</b></div></li>';
      statusEl.textContent = "不可用";
      renderCandidateDetailPanel(null, "BUY");
      return;
    }

    if (items.length <= 0) {
      detailRoot.classList.remove("panel-error");
      metaEl.textContent = "candidate focus";
      headEl.innerHTML = '<span class="tag wait">等待</span><span class="mono">点击候选卡片查看完整解释</span>';
      summaryEl.textContent = "这里会把单条候选的盘口、钱包、追价和题材 enrich 细节拆开给你看。";
      listEl.innerHTML = "<li><span>状态</span><b>等待候选数据...</b></li>";
      metricEl.innerHTML = '<div class="component-card"><span>候选深读</span><b>等待数据</b></div>';
      breakdownMetaEl.textContent = "0 factors";
      breakdownEl.innerHTML = '<div class="component-card factor-card factor-neutral"><span>score breakdown</span><b>等待数据</b><p>这里会展示综合分、钱包分、动量和盘口成本</p></div>';
      conflictMetaEl.textContent = "0 conflicts";
      conflictEl.innerHTML = '<div class="component-card factor-card factor-neutral"><span>风险冲突</span><b>等待数据</b><p>这里会展示已有仓位、重复、预算与冷却冲突</p></div>';
      factorsMetaEl.textContent = "0 factors";
      factorsEl.innerHTML = '<div class="component-card candidate-summary-card factor-card factor-neutral"><span>决策摘要</span><b>等待数据</b><p>这里会展示主结论、关键因子和风险摘要</p></div>';
      trailMetaEl.textContent = "0 steps";
      trailEl.innerHTML = '<li><div class="review-main"><span>复盘链路</span><b>等待数据...</b></div></li>';
      explainMetaEl.textContent = "0 lines";
      explainEl.innerHTML = '<li><div class="review-main"><span>说明</span><b>等待数据...</b></div></li>';
      snapshotEl.innerHTML = '<div class="component-card"><span>snapshot</span><b>等待数据...</b></div>';
      actionEl.innerHTML = '<li><div class="review-main"><span>补充记录</span><b>等待数据...</b></div></li>';
      statusEl.textContent = "等待数据...";
      renderCandidateDetailPanel(null, "BUY");
      return;
    }

    const availableIds = new Set(items.map((item) => candidateKey(item)));
    if (!availableIds.has(String(selectedCandidateId || ""))) {
      selectedCandidateId = candidateKey(items[0]);
      persistExitReviewUiState();
    }
    const candidate = selectedCandidate(payload) || items[0];
    const side = String(candidate.side || "BUY").toUpperCase();
    const [statusCls, statusText] = candidateStatusMeta(candidate.status);
    const explain = Array.isArray(candidate.explanation) && candidate.explanation.length > 0
      ? candidate.explanation.map((line) => ({
          label: String(line && line.label || "说明"),
          value: String(line && line.value || ""),
        }))
      : candidateExplanation(candidate, side);
    const signalSnapshotRows = candidateObjectRows(candidate.signal_snapshot || {}, 4);
    const topicSnapshotRows = candidateObjectRows(candidate.topic_snapshot || {}, 4);
    const currentBid = candidate.current_best_bid == null ? "--" : Number(candidate.current_best_bid).toFixed(3);
    const currentAsk = candidate.current_best_ask == null ? "--" : Number(candidate.current_best_ask).toFixed(3);
    const currentMid = candidate.current_midpoint == null ? "--" : Number(candidate.current_midpoint).toFixed(3);
    const spreadPct = candidate.spread_pct == null ? "--" : fmtPct(candidate.spread_pct, 1);
    const chasePct = candidate.chase_pct == null ? "--" : fmtPct(candidate.chase_pct, 1);
    const momentum5m = candidate.momentum_5m == null ? "--" : fmtSignedRatioPct(candidate.momentum_5m, 1);
    const momentum30m = candidate.momentum_30m == null ? "--" : fmtSignedRatioPct(candidate.momentum_30m, 1);
    const explainTitle = candidateExplainTitle(candidate);
    const score = Number(candidate.score || 0);
    const confidence = Number(candidate.confidence || 0);
    const hasConflict = candidate.existing_position_conflict === true || candidate.has_existing_position === true;
    const existingNotional = Number(candidate.existing_position_notional || 0);
    const ageSec = Math.max(0, Math.floor(Date.now() / 1000) - Number(candidate.updated_ts || candidate.created_ts || 0));
    const factorCards = candidateFactorCards(candidate, side);
    const factorStrip = candidateFocusStrip(candidate, side);
    const trail = candidateReviewTrail(candidate, side);
    const gateState = candidateGateState(candidate, side);

    metaEl.textContent = `${candidate.market_slug || candidate.token_id || "candidate"} · ${candidate.trigger_type || "wallet_event"} · ${candidateKey(candidate)}`;
    headEl.innerHTML =
      `<span class="tag ${statusCls}">${statusText}</span>` +
      `<span class="tag ${side === "SELL" ? "danger" : "ok"}">${side}</span>` +
      `<span class="tag ${gateState.gated ? gateState.tone : "ok"}">${gateState.gated ? gateState.label : "门禁通过"}</span>` +
      `<span class="tag ${candidate.suggested_action === "watch" ? "wait" : candidate.suggested_action === "ignore" ? "cancel" : "ok"}">${candidateActionText(candidate.suggested_action, side)}</span>` +
      `<span class="tag ${hasConflict ? "danger" : "ok"}">${hasConflict ? "已有冲突" : "无冲突"}</span>`;
    summaryEl.textContent = candidateCardSummaryLead(candidate, side);
    listEl.innerHTML = [
      `<li><span>一句话</span><b>${escapeHtml(candidateCardSummaryLead(candidate, side))}</b></li>`,
      `<li><span>执行门禁</span><b class="${gateState.gated ? "value-negative" : "value-positive"}">${escapeHtml(gateState.label)} · ${escapeHtml(gateState.reason || gateState.detail || "通过")}</b></li>`,
      `<li><span>来源钱包</span><b>${shortWallet(candidate.wallet)}${candidate.wallet_tag ? ` · ${candidate.wallet_tag}` : ""}${candidate.wallet_tier ? ` · ${candidate.wallet_tier}` : ""}</b></li>`,
      `<li><span>Condition / Token</span><b>${candidate.condition_id || "--"}${candidate.token_id ? ` · ${candidate.token_id}` : ""}</b></li>`,
      `<li><span>观察金额</span><b>${fmtUsd(candidate.observed_notional || 0, false)} · ${Number(candidate.observed_size || 0).toFixed(2)} 份</b></li>`,
      `<li><span>来源均价</span><b>${Number(candidate.source_avg_price || 0).toFixed(4)} · 当前 bid ${currentBid} / ask ${currentAsk} / mid ${currentMid}</b></li>`,
      `<li><span>盘口成本</span><b>spread ${spreadPct} · chase ${chasePct}</b></li>`,
      `<li><span>动量</span><b>5m ${momentum5m} · 30m ${momentum30m}</b></li>`,
      `<li><span>已有仓位</span><b>${hasConflict ? `冲突 ${fmtUsd(existingNotional, false)}` : "无冲突"}</b></li>`,
      `<li><span>建议动作</span><b>${candidateActionText(candidate.suggested_action, side)} · ${fmtPct(confidence * 100, 0)} 置信度</b></li>`,
      `<li><span>过期 / 新鲜度</span><b>${candidate.expires_ts > 0 ? `${fmtAge(Math.max(0, Number(candidate.expires_ts || 0) - Math.floor(Date.now() / 1000)))}` : "长期有效"} · ${fmtAge(ageSec)}前更新</b></li>`,
    ].join("");

    metricEl.innerHTML = [
      `<div class="component-card"><span>候选评分</span><b class="${clsForScore(score)}">${score.toFixed(1)}</b></div>`,
      `<div class="component-card"><span>钱包评分</span><b class="${clsForScore(candidate.wallet_score || 0)}">${Number(candidate.wallet_score || 0).toFixed(1)}</b></div>`,
      `<div class="component-card"><span>钱包分组</span><b>${candidate.wallet_tier || "WATCH"}</b></div>`,
      `<div class="component-card"><span>来源钱包数</span><b>${Number(candidate.source_wallet_count || 1)}</b></div>`,
    ].join("");
    breakdownMetaEl.textContent = `${factorCards.breakdown.length} factors`;
    breakdownEl.innerHTML = renderFactorCards(factorCards.breakdown);
    conflictMetaEl.textContent = `${factorCards.conflicts.length} conflicts`;
    conflictEl.innerHTML = renderFactorCards(factorCards.conflicts);
    factorsMetaEl.textContent = `${factorStrip.length} factors`;
    factorsEl.innerHTML = renderCandidateSummaryCards(factorStrip);
    trailMetaEl.textContent = `${trail.length} steps`;
    trailEl.innerHTML = trail.length > 0
      ? trail.map((item) => `<li>
          <div class="review-main">
            <span>${escapeHtml(item.label)}</span>
            <b class="${candidateFactorValueClass(item.tone)}">${escapeHtml(item.value)}</b>
          </div>
          <div class="review-sub">${escapeHtml(item.detail || "")}</div>
        </li>`).join("")
      : '<li><div class="review-main"><span>复盘链路</span><b>暂无链路</b></div></li>';

    explainMetaEl.textContent = `${explain.length} lines`;
    explainEl.innerHTML = explain.length > 0
      ? explain.map((line) => `<li><div class="review-main"><span>${escapeHtml(line.label)}</span><b>${escapeHtml(line.value || "")}</b></div></li>`).join("")
      : '<li><div class="review-main"><span>说明</span><b>暂无解释</b></div></li>';

    snapshotEl.innerHTML = [
      `<div class="component-card"><span>推荐理由</span><b>${escapeHtml(candidate.recommendation_reason || "暂无")}</b></div>`,
      `<div class="component-card"><span>市场标签</span><b>${escapeHtml(candidate.market_tag || "未标记")}</b></div>`,
      `<div class="component-card"><span>解析桶</span><b>${escapeHtml(candidate.resolution_bucket || "未标记")}</b></div>`,
      `<div class="component-card"><span>冲突</span><b class="${hasConflict ? "value-negative" : "value-positive"}">${hasConflict ? "existing position" : "clear"}</b></div>`,
    ].join("");

    const snapshotLines = [];
    if (signalSnapshotRows.length > 0) {
      snapshotLines.push(`<li><div class="review-main"><span>signal snapshot</span><b>${signalSnapshotRows.length} keys</b></div><div class="review-sub">${signalSnapshotRows.map((row) => `${escapeHtml(row.key)}=${escapeHtml(row.value)}`).join(" · ")}</div></li>`);
    }
    if (topicSnapshotRows.length > 0) {
      snapshotLines.push(`<li><div class="review-main"><span>topic snapshot</span><b>${topicSnapshotRows.length} keys</b></div><div class="review-sub">${topicSnapshotRows.map((row) => `${escapeHtml(row.key)}=${escapeHtml(row.value)}`).join(" · ")}</div></li>`);
    }
    if (candidate.note) {
      snapshotLines.push(`<li><div class="review-main"><span>备注</span><b>${escapeHtml(candidate.note)}</b></div><div class="review-sub">selected_action=${escapeHtml(candidate.selected_action || "none")}</div></li>`);
    }
    actionEl.innerHTML = snapshotLines.length > 0
      ? snapshotLines.join("")
      : '<li><div class="review-main"><span>补充记录</span><b>暂无额外记录</b></div></li>';
    statusEl.textContent = snapshotLines.length > 0 ? `${snapshotLines.length} items` : "无额外记录";
    detailRoot.dataset.side = side;
    renderCandidateDetailPanel(candidate, side);
  }

  function renderNotifierPanel(payload) {
    const data = payload && typeof payload === "object" ? payload : EMPTY_NOTIFIER;
    lastNotifierSummary = data;
    const metaEl = $("notifier-meta");
    const statusEl = $("notifier-status");
    const summaryEl = $("notifier-summary");
    const eventsEl = $("notifier-events");
    const detailEl = $("notifier-detail");
    if (!metaEl || !statusEl || !summaryEl || !eventsEl || !detailEl) return;

    const recent = Array.isArray(data.recent) ? data.recent.slice(0, 5) : [];
    const channels = Array.isArray(data.channels) ? data.channels : [];
    const deliveryStats = data.delivery_stats && typeof data.delivery_stats === "object" ? data.delivery_stats : EMPTY_NOTIFIER.delivery_stats;
    const webhookChannel = channels.find((item) => String(item && item.name || "").trim().toLowerCase() === "webhook");
    const telegramChannel = channels.find((item) => String(item && item.name || "").trim().toLowerCase() === "telegram");
    const configuredParts = [];
    if (data.local_available) configuredParts.push("local");
    if (data.webhook_configured) configuredParts.push(`webhook${webhookChannel && webhookChannel.target_count ? `×${Number(webhookChannel.target_count || 0)}` : ""}`);
    if (data.telegram_configured) configuredParts.push("telegram");
    metaEl.textContent = `${recent.length} recent`;
    statusEl.textContent = configuredParts.length > 0
      ? `${configuredParts.join(" + ")} · ${data.updated_ts > 0 ? fmtDateTime(data.updated_ts) : "waiting"}`
      : "no notifier backend configured";
    summaryEl.innerHTML = [
      `<div class="component-card"><span>本地通知</span><b>${data.local_available ? "可用" : "不可用"}</b></div>`,
      `<div class="component-card"><span>Webhook</span><b>${data.webhook_configured ? `已配置${webhookChannel && webhookChannel.target_count ? ` · ${Number(webhookChannel.target_count || 0)} targets` : ""}` : "未配置"}</b></div>`,
      `<div class="component-card"><span>Telegram</span><b>${data.telegram_configured ? "已配置" : "未配置"}</b></div>`,
      `<div class="component-card"><span>投递统计</span><b>${Number(deliveryStats.ok_events || 0)} / ${Number(deliveryStats.event_count || 0)} OK</b></div>`,
      `<div class="component-card"><span>更新时间</span><b>${data.updated_ts > 0 ? fmtDateTime(data.updated_ts) : "--"}</b></div>`,
    ].join("");
    detailEl.innerHTML = recent.length > 0
      ? recent.map((item) => {
          const [cls, label] = item.ok ? ["ok", "OK"] : ["danger", "FAIL"];
          const backend = String(item.backend || item.channel || "local");
          const title = String(item.title || "通知");
          const body = String(item.body || item.detail || "");
          const deliveryCount = Number(item.delivery_count || (Array.isArray(item.deliveries) ? item.deliveries.length : 0) || 0);
          return `<li>
            <div class="review-main">
              <span>${escapeHtml(title)}</span>
              <b><span class="tag ${cls}">${label}</span> <span class="tag wait">${escapeHtml(backend)}</span></b>
            </div>
            <div class="review-sub">${escapeHtml(body)}${deliveryCount > 0 ? ` · deliveries ${deliveryCount}` : ""}${item.status_code ? ` · ${item.status_code}` : ""}${item.ts ? ` · ${fmtDateTime(item.ts)}` : ""}</div>
          </li>`;
        }).join("")
      : '<li><div class="review-main"><span>最近通知</span><b>暂无事件</b></div></li>';
    eventsEl.innerHTML = recent.length > 0
      ? recent.map((item) => `<div class="component-card">
          <span>${escapeHtml(String(item.channel || item.backend || "local"))}</span>
          <b>${escapeHtml(String(item.title || "通知"))}</b>
          <p>${escapeHtml(String(item.body || item.detail || "no body"))}</p>
        </div>`).join("")
      : channels.length > 0
        ? channels.map((item) => `<div class="component-card">
            <span>${escapeHtml(String(item.name || "channel"))}</span>
            <b>${item.configured ? "已配置" : "未配置"}</b>
            <p>${escapeHtml(item.name === "webhook"
              ? `${Number(item.target_count || 0)} targets`
              : item.name === "telegram"
                ? `${String(item.chat_id || "chat unavailable")} · ${String(item.parse_mode || "plain").trim() || "plain"}`
                : String(item.backend || "local"))}</p>
          </div>`).join("")
        : '<div class="component-card"><span>最近通知</span><b>暂无事件</b></div>';
  }

  function renderArchivePanel(data, monitor30m, monitor12h, reconciliationEod, now) {
    const metaEl = $("archive-meta");
    const summaryEl = $("archive-summary");
    const actionsEl = $("archive-actions");
    const statsEl = $("archive-stats");
    const tagsEl = $("archive-tags");
    const snapshotsEl = $("archive-snapshots");
    const recentEl = $("archive-recent");
    if (!metaEl || !summaryEl || !actionsEl || !statsEl || !tagsEl || !snapshotsEl || !recentEl) return;

    const state = data && typeof data === "object" ? data : {};
    const summary = state.summary || {};
    const candidates = state.candidates || EMPTY_CANDIDATES;
    const journal = state.journal_summary || EMPTY_JOURNAL;
    const notifier = state.notifier || lastNotifierSummary || EMPTY_NOTIFIER;
    const walletProfiles = state.wallet_profiles || EMPTY_WALLET_PROFILES;
    const monitor30 = monitor30m || EMPTY_MONITOR_REPORT("monitor_30m");
    const monitor12 = monitor12h || EMPTY_MONITOR_REPORT("monitor_12h");
    const eod = reconciliationEod || EMPTY_RECONCILIATION_EOD_REPORT;
    const eodRecon = eod.reconciliation && typeof eod.reconciliation === "object" ? eod.reconciliation : {};
    const topTags = Array.isArray(journal.top_tags) ? journal.top_tags : [];
    const recentNotes = Array.isArray(journal.recent) ? journal.recent : [];
    const notifierRecent = Array.isArray(notifier.recent) ? notifier.recent : [];
    const candidateCount = Number(candidates.summary && candidates.summary.count || 0);
    const pendingCount = Number(candidates.summary && candidates.summary.pending || 0);
    const journalCount = Number(journal.count || journal.total_entries || 0);
    const notifierCount = Number(notifierRecent.length || 0);
    const openPositions = Number(summary.open_positions || 0);
    const slotUtil = Number(summary.slot_utilization_pct || summary.exposure_pct || 0);
    const monitor30Tag = reportDecisionMeta(monitor30.final_recommendation || monitor30.recommendation, monitor30.reconciliation_status);
    const monitor12Tag = reportDecisionMeta(monitor12.final_recommendation || monitor12.recommendation, monitor12.reconciliation_status);
    const eodTag = reportStatusMeta(eod.status || eodRecon.status || "unknown");
    const eodGap = Number(eodRecon.internal_vs_ledger_diff || 0);
    const report30Freshness = reportFreshnessMeta(monitor30, 30 * 60, now);
    const report12Freshness = reportFreshnessMeta(monitor12, 12 * 60 * 60, now);
    const eodFreshness = reportFreshnessMeta(eod, 24 * 60 * 60, now);

    metaEl.textContent = [
      `state ${Number(data && data.ts || 0) > 0 ? historyAgeLabel(data.ts, now) : "未记录"}`,
      `30m ${report30Freshness.ageLabel}${report30Freshness.stale ? " · STALE" : ""}`,
      `12h ${report12Freshness.ageLabel}${report12Freshness.stale ? " · STALE" : ""}`,
      `EOD ${eodFreshness.ageLabel}`,
    ].join(" · ");

    summaryEl.innerHTML = [
      `<div class="review-chip"><span>候选</span><b>${candidateCount}</b></div>`,
      `<div class="review-chip"><span>待审批</span><b>${pendingCount}</b></div>`,
      `<div class="review-chip"><span>日记</span><b>${journalCount}</b></div>`,
      `<div class="review-chip"><span>通知</span><b>${notifierCount}</b></div>`,
      `<div class="review-chip"><span>持仓</span><b>${openPositions}</b></div>`,
      `<div class="review-chip"><span>槽位</span><b>${fmtPct(slotUtil, 1)}</b></div>`,
    ].join("");

    actionsEl.innerHTML = [
      { label: "导出状态包 JSON", action: "state_bundle", detail: "包含 state / monitor / reconciliation / candidates / journal / notifier" },
      { label: "导出候选 CSV", action: "candidates_csv", detail: "用于离线筛选和复盘候选机会" },
      { label: "导出日记 JSON", action: "journal_json", detail: "包含最近笔记、标签和动作摘要" },
      { label: "导出监控包 JSON", action: "monitor_bundle", detail: "包含 30m / 12h / EOD 对账结果" },
    ].map((item) => `
      <button class="btn export-btn" type="button" data-export-action="${item.action}">
        <span>${item.label}</span>
        <small>${item.detail}</small>
      </button>
    `).join("");

    statsEl.innerHTML = [
      `<div class="component-card"><span>30m / 12h</span><b><span class="tag ${monitor30Tag[0]}">${monitor30Tag[1]}</span> <span class="tag ${monitor12Tag[0]}">${monitor12Tag[1]}</span></b><p>${monitorWindowDisplaySummary(monitor30, 30 * 60, now)} / ${monitorWindowDisplaySummary(monitor12, 12 * 60 * 60, now)}</p></div>`,
      `<div class="component-card"><span>EOD 对账</span><b><span class="tag ${eodTag[0]}">${eodTag[1]}</span></b><p>internal vs ledger ${fmtUsd(eodGap)} · fills ${Number(eodRecon.fill_count_today || 0)}</p></div>`,
      `<div class="component-card"><span>钱包池</span><b>${Number(walletProfiles.summary && walletProfiles.summary.count || 0)} profiles</b><p>enabled ${Number(walletProfiles.summary && walletProfiles.summary.enabled || 0)} · notifier ${notifier.local_available ? "local on" : "local off"}</p></div>`,
      `<div class="component-card"><span>最近通知</span><b>${notifierCount} events</b><p>${notifier.webhook_configured ? "webhook on" : "webhook off"} · ${notifier.telegram_configured ? "telegram on" : "telegram off"} · latest ${notifier.last && notifier.last.title ? String(notifier.last.title) : "none"}</p></div>`,
    ].join("");

    tagsEl.innerHTML = topTags.length > 0
      ? topTags.map((tag) => `<span class="tag wait">${escapeHtml(String(tag.tag || tag.label || "tag"))} · ${Number(tag.count || 0)}</span>`).join("")
      : '<span class="tag wait">暂无 journal 标签</span>';

    snapshotsEl.innerHTML = [
      `<div class="component-card"><span>最近 EOD</span><b>${eod.day_key || "--"}</b><p>${Number(eod.generated_ts || 0) > 0 ? `${fmtDateTime(eod.generated_ts)} · ${historyAgeLabel(eod.generated_ts, now)}` : "未生成"} · ${String(eod.status || "unknown")}</p></div>`,
      `<div class="component-card"><span>monitor 30m</span><b>${String(monitor30.sample_status || "unknown")}</b><p>${monitorWindowDisplaySummary(monitor30, 30 * 60, now)}</p></div>`,
      `<div class="component-card"><span>monitor 12h</span><b>${String(monitor12.sample_status || "unknown")}</b><p>${monitorWindowDisplaySummary(monitor12, 12 * 60 * 60, now)}</p></div>`,
      `<div class="component-card"><span>日记最近</span><b>${recentNotes.length}</b><p>${recentNotes[0] ? String(recentNotes[0].text || recentNotes[0].rationale || "").slice(0, 80) : "暂无日记"}</p></div>`,
    ].join("");

    recentEl.innerHTML = notifierRecent.length > 0
      ? notifierRecent.map((item) => {
          const [cls, tag] = item.ok ? ["ok", "OK"] : ["danger", "FAIL"];
          return `<li>
            <div class="review-main">
              <span>${escapeHtml(String(item.title || item.channel || "通知"))}</span>
              <b><span class="tag ${cls}">${tag}</span> <span class="tag wait">${escapeHtml(String(item.backend || item.channel || "local"))}</span></b>
            </div>
            <div class="review-sub">${escapeHtml(String(item.body || item.detail || ""))}${item.status_code ? ` · ${item.status_code}` : ""}${item.ts ? ` · ${fmtDateTime(item.ts)}` : ""}</div>
          </li>`;
        }).join("")
      : '<li><div class="review-main"><span>最近通知</span><b>暂无事件</b></div></li>';
  }

  async function exportJsonSnapshot(filename, payloadPromise) {
    const payload = typeof payloadPromise === "function" ? await payloadPromise() : payloadPromise;
    downloadJson(filename, payload);
  }

  async function exportCsvSnapshot(filename, rows, columns) {
    downloadCsv(filename, rows, columns);
  }

  function candidateExportRows(payload) {
    const items = Array.isArray(payload && payload.items) ? payload.items : [];
    return items.map((candidate) => ({
      id: candidateKey(candidate),
      wallet: String(candidate.wallet || ""),
      market_slug: String(candidate.market_slug || ""),
      outcome: String(candidate.outcome || ""),
      side: String(candidate.side || ""),
      trigger_type: String(candidate.trigger_type || ""),
      suggested_action: String(candidate.suggested_action || ""),
      status: String(candidate.status || ""),
      wallet_score: Number(candidate.wallet_score || 0).toFixed(1),
      score: Number(candidate.score || 0).toFixed(1),
      confidence: fmtPct(Number(candidate.confidence || 0) * 100, 0),
      observed_notional: fmtUsd(candidate.observed_notional || 0, false),
      source_avg_price: Number(candidate.source_avg_price || 0).toFixed(4),
      current_best_bid: candidate.current_best_bid == null ? "" : Number(candidate.current_best_bid).toFixed(4),
      current_best_ask: candidate.current_best_ask == null ? "" : Number(candidate.current_best_ask).toFixed(4),
      current_midpoint: candidate.current_midpoint == null ? "" : Number(candidate.current_midpoint).toFixed(4),
      spread_pct: candidate.spread_pct == null ? "" : fmtPct(candidate.spread_pct, 2),
      chase_pct: candidate.chase_pct == null ? "" : fmtPct(candidate.chase_pct, 2),
      market_tag: String(candidate.market_tag || ""),
      resolution_bucket: String(candidate.resolution_bucket || ""),
      note: String(candidate.note || candidate.skip_reason || ""),
    }));
  }

  async function buildStateBundle() {
    const [state, monitor30m, monitor12h, reconciliationEod, candidates, journal, notifier, walletProfiles] = await Promise.all([
      fetchJson("/api/state", null),
      fetchJson("/api/monitor/30m", EMPTY_MONITOR_REPORT("monitor_30m")),
      fetchJson("/api/monitor/12h", EMPTY_MONITOR_REPORT("monitor_12h")),
      fetchJson("/api/reconciliation/eod", EMPTY_RECONCILIATION_EOD_REPORT),
      fetchJsonState("/api/candidates", EMPTY_CANDIDATES),
      fetchJsonState("/api/journal", EMPTY_JOURNAL),
      Promise.resolve(lastNotifierSummary || EMPTY_NOTIFIER),
      fetchJsonState("/api/wallet-profiles", EMPTY_WALLET_PROFILES),
    ]);
    return {
      generated_ts: Math.floor(Date.now() / 1000),
      state,
      monitor_30m: monitor30m,
      monitor_12h: monitor12h,
      reconciliation_eod: reconciliationEod,
      candidates: candidates && candidates.data ? candidates.data : EMPTY_CANDIDATES,
      journal: journal && journal.data ? journal.data : EMPTY_JOURNAL,
      notifier: state && state.notifier ? state.notifier : notifier,
      wallet_profiles: walletProfiles && walletProfiles.data ? walletProfiles.data : EMPTY_WALLET_PROFILES,
    };
  }

  function bindArchiveActions() {
    const archiveRoot = $("archive-panel");
    if (!archiveRoot || archiveRoot.dataset.bound === "1") return;
    archiveRoot.dataset.bound = "1";
    archiveRoot.addEventListener("click", async (event) => {
      const button = event.target.closest("[data-export-action]");
      if (!button) return;
      const action = String(button.getAttribute("data-export-action") || "").trim();
      if (!action) return;
      setButtonBusy(button, true);
      try {
        if (action === "state_bundle") {
          await exportJsonSnapshot(`poly_state_bundle_${Date.now()}.json`, buildStateBundle);
        } else if (action === "candidates_csv") {
          await exportCsvSnapshot(
            `poly_candidates_${Date.now()}.csv`,
            candidateExportRows(lastDecisionCandidates || EMPTY_CANDIDATES),
            [
              { key: "id", label: "id", value: (row) => row.id },
              { key: "wallet", label: "wallet", value: (row) => row.wallet },
              { key: "market_slug", label: "market_slug", value: (row) => row.market_slug },
              { key: "outcome", label: "outcome", value: (row) => row.outcome },
              { key: "side", label: "side", value: (row) => row.side },
              { key: "trigger_type", label: "trigger_type", value: (row) => row.trigger_type },
              { key: "suggested_action", label: "suggested_action", value: (row) => row.suggested_action },
              { key: "status", label: "status", value: (row) => row.status },
              { key: "wallet_score", label: "wallet_score", value: (row) => row.wallet_score },
              { key: "score", label: "score", value: (row) => row.score },
              { key: "confidence", label: "confidence", value: (row) => row.confidence },
              { key: "observed_notional", label: "observed_notional", value: (row) => row.observed_notional },
              { key: "source_avg_price", label: "source_avg_price", value: (row) => row.source_avg_price },
              { key: "current_best_bid", label: "current_best_bid", value: (row) => row.current_best_bid },
              { key: "current_best_ask", label: "current_best_ask", value: (row) => row.current_best_ask },
              { key: "current_midpoint", label: "current_midpoint", value: (row) => row.current_midpoint },
              { key: "spread_pct", label: "spread_pct", value: (row) => row.spread_pct },
              { key: "chase_pct", label: "chase_pct", value: (row) => row.chase_pct },
              { key: "market_tag", label: "market_tag", value: (row) => row.market_tag },
              { key: "resolution_bucket", label: "resolution_bucket", value: (row) => row.resolution_bucket },
              { key: "note", label: "note", value: (row) => row.note },
            ]
          );
        } else if (action === "journal_json") {
          await exportJsonSnapshot(`poly_journal_${Date.now()}.json`, {
            generated_ts: Math.floor(Date.now() / 1000),
            journal: lastJournalSummary || EMPTY_JOURNAL,
          });
        } else if (action === "monitor_bundle") {
          await exportJsonSnapshot(`poly_monitor_bundle_${Date.now()}.json`, async () => {
            const [state, monitor30m, monitor12h, reconciliationEod] = await Promise.all([
              fetchJson("/api/state", null),
              fetchJson("/api/monitor/30m", EMPTY_MONITOR_REPORT("monitor_30m")),
              fetchJson("/api/monitor/12h", EMPTY_MONITOR_REPORT("monitor_12h")),
              fetchJson("/api/reconciliation/eod", EMPTY_RECONCILIATION_EOD_REPORT),
            ]);
            return {
              generated_ts: Math.floor(Date.now() / 1000),
              state,
              monitor_30m: monitor30m,
              monitor_12h: monitor12h,
              reconciliation_eod: reconciliationEod,
            };
          });
        }
      } finally {
        setButtonBusy(button, false);
      }
    });
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

  async function fetchJsonState(path, fallback) {
    try {
      const res = await fetch(path, { cache: "no-store" });
      if (!res.ok) {
        return {
          ok: false,
          data: fallback,
          error: `${path} ${res.status}`,
        };
      }
      const data = await res.json();
      if (!data || typeof data !== "object") {
        return {
          ok: false,
          data: fallback,
          error: `${path} invalid payload`,
        };
      }
      return {
        ok: true,
        data,
        error: "",
      };
    } catch (_err) {
      return {
        ok: false,
        data: fallback,
        error: `${path} unavailable`,
      };
    }
  }

  async function postJson(path, body) {
    const res = await fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    });
    const payload = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(`request failed: ${res.status}`);
    }
    return payload;
  }

  async function pushMode(mode) {
    const payload = await postJson("/api/mode", { mode });
    const next = payload && payload.decision_mode ? payload.decision_mode : { mode };
    controlState.decision_mode = String(next.mode || mode || "manual");
    return next;
  }

  async function pushCandidateAction(candidateId, action, note = "") {
    return postJson("/api/candidate/action", {
      candidate_id: candidateId,
      action,
      note,
    });
  }

  function renderStatusMessage(id, meta, fallbackText) {
    const el = $(id);
    if (!el) return;
    const cls = String(meta && meta.cls || "wait");
    const text = String(meta && meta.text || fallbackText || "");
    el.className = `api-status ${cls}`;
    el.textContent = text || fallbackText || "";
  }

  function walletProfileSource(wallet) {
    const items = Array.isArray(lastWalletProfiles && lastWalletProfiles.items) ? lastWalletProfiles.items : [];
    const target = normalizeWallet(wallet);
    return items.find((item) => normalizeWallet(item && item.wallet) === target) || null;
  }

  function walletProfileDraft(item) {
    const wallet = normalizeWallet(item && item.wallet);
    const draft = wallet ? walletProfileDrafts[wallet] : null;
    if (draft && typeof draft === "object") {
      return {
        tag: String(draft.tag || ""),
        notes: String(draft.notes || ""),
        enabled: !!draft.enabled,
      };
    }
    return {
      tag: String(item && item.tag || ""),
      notes: String(item && item.notes || ""),
      enabled: !!(item && item.enabled !== false),
    };
  }

  function walletProfileChanged(item, draft) {
    if (!item) return false;
    return String(draft.tag || "").trim() !== String(item.tag || "").trim()
      || String(draft.notes || "").trim() !== String(item.notes || "").trim()
      || !!draft.enabled !== !!(item.enabled !== false);
  }

  function decisionPanelStatus(payload, decision, apiState) {
    if (String(decisionConsoleNotice.text || "").trim()) return decisionConsoleNotice;
    if (!apiState.candidates.ok || !apiState.mode.ok) {
      const parts = [];
      if (!apiState.candidates.ok) parts.push(`candidates ${apiState.candidates.error || "unavailable"}`);
      if (!apiState.mode.ok) parts.push(`mode ${apiState.mode.error || "unavailable"}`);
      return {
        cls: "danger",
        text: `机会队列接口异常: ${parts.join(" · ")}`,
      };
    }
    const count = Number(payload && payload.summary && payload.summary.count || 0);
    return {
      cls: count > 0 ? "ok" : "wait",
      text: count > 0
        ? `当前为 ${modeLabel(decision && decision.mode)} 模式，优先处理 suggested_action 和解释区。`
        : "当前没有新机会，继续观察下方 operator / monitor / reconciliation 面板。",
    };
  }

  function renderDecisionConsole(candidatesPayload, decisionModePayload, apiState = {}) {
    const payload = candidatesPayload && typeof candidatesPayload === "object" ? candidatesPayload : EMPTY_CANDIDATES;
    const decision = decisionModePayload && typeof decisionModePayload === "object" ? decisionModePayload : EMPTY_DECISION_MODE;
    lastDecisionCandidates = payload;
    lastDecisionMode = decision;
    lastDecisionApiState = {
      candidates: apiState.candidates && typeof apiState.candidates === "object" ? apiState.candidates : { ok: true, error: "" },
      mode: apiState.mode && typeof apiState.mode === "object" ? apiState.mode : { ok: true, error: "" },
    };
    const allItems = Array.isArray(payload.items) ? payload.items.slice(0, 32) : [];
    const filteredItems = candidateSortCandidates(
      allItems.filter((candidate) => candidateMatchesQueueFilter(candidate, candidateQueueFilter)),
      candidateQueueFilter.sort
    );
    const compactCards = filteredItems.length >= 8;
    const candidatePanelEl = $("candidate-panel");
    if (candidatePanelEl) {
      candidatePanelEl.classList.toggle("is-compact", compactCards);
    }
    const summary = payload.summary || {};
    const mode = String(decision.mode || "manual");
    controlState.decision_mode = mode;
    renderStatusMessage("candidate-panel-status", decisionPanelStatus(payload, decision, lastDecisionApiState), "等待 candidates / mode 数据...");

    if ($("candidate-meta")) {
      const updatedTs = Number(decision.updated_ts || payload.updated_ts || 0);
      $("candidate-meta").textContent = `${modeLabel(mode)} · ${allItems.length} candidates · ${filteredItems.length} visible · ${candidateSortLabel(candidateQueueFilter.sort)}${updatedTs > 0 ? ` · ${fmtDateTime(updatedTs)}` : ""}`;
    }
    if ($("candidate-grid-meta")) {
      $("candidate-grid-meta").textContent = `${filteredItems.length}/${allItems.length} visible · ${candidateFocusViewLabel(candidateFocusView)}`;
    }
    if ($("candidate-summary")) {
      const visibleSummary = candidateSummaryCounts(filteredItems);
      $("candidate-summary").innerHTML = [
        `<span class="chip active">待处理 ${Number(visibleSummary.pending || 0)}</span>`,
        `<span class="chip">已批准 ${Number(visibleSummary.approved || 0)}</span>`,
        `<span class="chip">观察中 ${Number(visibleSummary.watched || 0)}</span>`,
        `<span class="chip">已执行 ${Number(visibleSummary.executed || 0)}</span>`,
        `<span class="chip">可见 ${filteredItems.length}/${allItems.length}</span>`,
      ].join("");
    }
    const filterMetaEl = $("candidate-filter-meta");
    if (filterMetaEl) {
      filterMetaEl.textContent = candidateFilterSummary(candidateQueueFilter, filteredItems.length, allItems.length);
    }
    renderCandidateFilterToggle();
    const switchEl = $("decision-mode-switch");
    if (switchEl) {
      switchEl.querySelectorAll("[data-mode]").forEach((btn) => {
        btn.classList.toggle("active", String(btn.getAttribute("data-mode") || "") === mode);
      });
    }
    const searchInput = $("candidate-search-input");
    if (searchInput && searchInput !== document.activeElement) {
      searchInput.value = String(candidateQueueFilter.search || "");
    }
    const sortSelect = $("candidate-sort-select");
    if (sortSelect && sortSelect.value !== String(candidateQueueFilter.sort || "score_desc")) {
      sortSelect.value = String(candidateQueueFilter.sort || "score_desc");
    }
    renderCandidateFocusViewSwitch();
    document.querySelectorAll("[data-candidate-filter]").forEach((button) => {
      const key = String(button.getAttribute("data-candidate-filter") || "").trim();
      const value = String(button.getAttribute("data-filter-value") || "").trim();
      const activeValue = String(candidateQueueFilter[key] || "all").trim();
      button.classList.toggle("active", value === activeValue);
    });
    const grid = $("candidate-grid");
    const visiblePayload = { ...payload, items: filteredItems };
    const activeCandidateId = candidateKey(selectedCandidate(visiblePayload) || filteredItems[0] || {});
    if (!grid) {
      renderCandidateFocus(visiblePayload, lastDecisionApiState);
      return;
    }
    if (!lastDecisionApiState.candidates.ok && filteredItems.length <= 0) {
      grid.innerHTML = `<article class="candidate-card candidate-empty candidate-error"><div><h3>候选接口暂时不可用</h3><p>${escapeHtml(lastDecisionApiState.candidates.error || "无法加载 /api/candidates")}</p></div></article>`;
      renderCandidateFocus(visiblePayload, lastDecisionApiState);
      return;
    }
    if (filteredItems.length <= 0) {
      const hasFilters = Boolean(String(candidateQueueFilter.search || "").trim()) || String(candidateQueueFilter.status || "all") !== "all" || String(candidateQueueFilter.action || "all") !== "all" || String(candidateQueueFilter.side || "all") !== "all";
      grid.innerHTML = `<article class="candidate-card candidate-empty"><div><h3>${hasFilters ? "没有匹配的候选" : "当前没有新机会"}</h3><p>${hasFilters ? "换个关键词、状态或排序再看一次。" : "没有候选信号时，页面会继续保留现有的执行/对账/监控面板。"}</p></div></article>`;
      renderCandidateFocus(visiblePayload, lastDecisionApiState);
      return;
    }

    grid.innerHTML = filteredItems.map((candidate) => {
      const candidateId = candidateKey(candidate);
      const side = String(candidate.side || "BUY").toUpperCase();
      const status = candidateStatusValue(candidate);
      const [statusCls, statusText] = candidateStatusMeta(status);
      const gateState = candidateGateState(candidate, side);
      const priorityItems = candidateCardPriorityItems(candidate, side);
      const reason = candidateCardShortSummary(candidate, side);
      const hoverText = candidateCardHoverText(candidate, side, reason, priorityItems);
      const actionPlan = candidateCardActionButtons(candidate, side);
      const hasAction = actionPlan.canOperate && actionPlan.primary;
      const primaryAction = actionPlan.primary;
      const actionState = candidateRequestState[candidateId] || {};
      const actionStatusCls = actionPlan.canOperate
        ? (actionState.kind === "error" ? "danger" : actionState.kind === "success" ? "ok" : actionState.kind === "pending" ? "wait" : "cancel")
        : "cancel";
      const actionStatusText = actionState.kind === "error"
        ? String(actionState.message || "提交失败")
        : actionState.kind === "success"
          ? String(actionState.message || "已提交")
          : actionState.kind === "pending"
            ? String(actionState.message || "提交中...")
            : "";
      const secondaryActions = Array.isArray(actionPlan.secondary) ? actionPlan.secondary : [];
      const secondaryButtons = hasAction ? secondaryActions.map((button) => `
              <button class="candidate-action ${button.cls || "subtle"}" type="button" data-action="${button.action}">${button.label}</button>`).join("") : "";
      const showAction = hasAction || secondaryActions.length > 0;
      const cardMeta = [];
      if (gateState.gated) {
        cardMeta.push(`<span class="tag ${gateState.tone}">门禁 · ${escapeHtml(gateState.reason)}</span>`);
      }
      if (actionStatusText) {
        cardMeta.push(`<span class="candidate-card-status tag ${actionStatusCls}">${escapeHtml(actionStatusText)}</span>`);
      }
      const cardCompactCls = compactCards ? " candidate-card-compact" : "";
      const cardGateCls = gateState.gated ? " gated" : "";
      return `
        <article class="candidate-card${cardCompactCls}${cardGateCls}${candidateId && candidateId === activeCandidateId ? " active" : ""}" data-candidate-id="${candidateId}" data-candidate-side="${side}" title="${escapeHtml(hoverText)}">
          <div class="candidate-card-top">
            <div class="candidate-card-top-main">
              <h3>${escapeHtml(candidate.market_slug || candidate.token_id || "-")}</h3>
              <p class="candidate-card-kicker candidate-card-reason">${escapeHtml(reason)}</p>
              ${gateState.gated ? `<p class="candidate-card-gate candidate-card-gate-${gateState.tone}">${escapeHtml(gateState.label)} · ${escapeHtml(gateState.reason)}</p>` : ""}
            </div>
            <div class="candidate-card-badges">
              <span class="tag ${side === "SELL" ? "danger" : "ok"}">${side}</span>
              <span class="tag ${statusCls}">${statusText}</span>
              ${cardMeta.join("")}
            </div>
          </div>
          <div class="candidate-card-strip">
            ${priorityItems.map((item) => `<div class="candidate-strip-item"><span>${escapeHtml(item.label)}</span><b>${escapeHtml(item.value)}</b></div>`).join("")}
          </div>
          <div class="candidate-card-actions">
            ${showAction
      ? hasAction
        ? `<button class="candidate-action primary" type="button" data-action="${primaryAction}" ${actionPlan.canOperate ? "" : "disabled"}>${candidateActionText(primaryAction, side)}</button>`
        : ""
      : `<span class="mono">${statusText ? `状态 ${statusText}` : "暂无可执行动作"}</span>`}
            ${showAction ? `<div class="candidate-card-actions-secondary">${secondaryButtons}</div>` : ""}
          </div>
        </article>
      `;
    }).join("");
    renderCandidateFocus(visiblePayload, lastDecisionApiState);
  }

  function renderWalletProfilesPanel(payload, apiState = { ok: true, error: "" }) {
    const data = payload && typeof payload === "object" ? payload : EMPTY_WALLET_PROFILES;
    lastWalletProfiles = data;
    lastWalletProfilesApiState = apiState && typeof apiState === "object" ? apiState : { ok: true, error: "" };
    const items = Array.isArray(data.items) ? data.items.slice(0, 8) : [];
    if ($("wallet-profiles-meta")) {
      const enabled = Number(data.summary && data.summary.enabled || 0);
      $("wallet-profiles-meta").textContent = `${items.length} wallets · enabled ${enabled}`;
    }
    renderStatusMessage(
      "wallet-profiles-status",
      !lastWalletProfilesApiState.ok
        ? { cls: "danger", text: `钱包池接口异常: ${lastWalletProfilesApiState.error || "wallet profiles unavailable"}` }
        : { cls: "wait", text: "支持直接改 tag / notes / enabled，保存后写回 wallet profile store。" },
      "等待钱包池数据..."
    );
    replaceRows(
      $("wallet-profiles-body"),
      items.map((item) => {
        const wallet = normalizeWallet(item.wallet);
        const draft = walletProfileDraft(item);
        const dirty = walletProfileChanged(item, draft);
        const requestState = walletProfileRequestState[wallet] || {};
        const requestLabel = requestState.kind === "error"
          ? String(requestState.message || "保存失败")
          : requestState.kind === "success"
            ? String(requestState.message || "已保存")
            : requestState.kind === "pending"
              ? String(requestState.message || "保存中...")
              : dirty ? "有未保存改动" : "已同步";
        const requestCls = requestState.kind === "error" ? "danger" : requestState.kind === "success" ? "ok" : requestState.kind === "pending" ? "wait" : dirty ? "wait" : "cancel";
        return `
          <tr data-wallet-profile="${attrToken(wallet)}">
            <td class="wrap"><div class="cell-stack"><span class="cell-main">${shortWallet(item.wallet)}</span><span class="cell-sub">${escapeHtml(item.category || "未分组")}</span></div></td>
            <td><label class="inline-toggle"><input type="checkbox" data-wallet-profile-field="enabled" data-wallet-profile-key="${attrToken(wallet)}" ${draft.enabled ? "checked" : ""} /><span>${draft.enabled ? "启用" : "停用"}</span></label></td>
            <td><input class="editor-input wallet-profile-input" type="text" data-wallet-profile-field="tag" data-wallet-profile-key="${attrToken(wallet)}" value="${escapeHtml(draft.tag)}" placeholder="标签" /></td>
            <td class="wrap"><div class="cell-stack"><span class="cell-main ${clsForScore(item.trust_score)}">${Number(item.trust_score || 0).toFixed(1)}</span><span class="cell-sub">follow ${Number(item.followability_score || 0).toFixed(1)}</span></div></td>
            <td class="wrap"><input class="editor-input wallet-profile-input" type="text" data-wallet-profile-field="notes" data-wallet-profile-key="${attrToken(wallet)}" value="${escapeHtml(draft.notes)}" placeholder="备注" /></td>
            <td class="wrap">
              <div class="wallet-profile-actions">
                <button class="btn btn-mini" type="button" data-wallet-profile-save="${attrToken(wallet)}">保存</button>
                <span class="mono wallet-profile-row-status ${requestCls === "danger" ? "value-negative" : requestCls === "ok" ? "value-positive" : "value-neutral"}">${escapeHtml(requestLabel)}</span>
              </div>
            </td>
          </tr>
        `;
      }),
      !lastWalletProfilesApiState.ok
        ? '<tr><td colspan="6">wallet profiles API 暂不可用</td></tr>'
        : '<tr><td colspan="6">暂无钱包画像</td></tr>'
    );
  }

  function renderJournalPanel(payload, apiState = { ok: true, error: "" }) {
    const data = payload && typeof payload === "object" ? payload : EMPTY_JOURNAL;
    lastJournalSummary = data;
    lastJournalApiState = apiState && typeof apiState === "object" ? apiState : { ok: true, error: "" };
    const items = Array.isArray(data.recent) ? data.recent.slice(0, 6) : [];
    if ($("journal-meta")) {
      $("journal-meta").textContent = `最近 ${items.length} 条`;
    }
    renderStatusMessage(
      "journal-panel-status",
      !lastJournalApiState.ok
        ? { cls: "danger", text: `交易日记接口异常: ${lastJournalApiState.error || "journal unavailable"}` }
        : { cls: "wait", text: `最近 ${Number(data.total_entries || 0)} 条记录，支持快速写一句理由。` },
      "等待日记数据..."
    );
    renderStatusMessage("journal-note-status", journalComposerNotice, "支持快速写一句理由");
    const grid = $("journal-grid");
    if (!grid) return;
    if (!lastJournalApiState.ok && items.length <= 0) {
      grid.innerHTML = `<div class="component-card journal-card journal-card-error"><span>交易日记</span><b>接口暂不可用</b><p>${escapeHtml(lastJournalApiState.error || "无法加载 /api/journal")}</p></div>`;
      return;
    }
    if (items.length <= 0) {
      grid.innerHTML = '<div class="component-card journal-card"><span>交易日记</span><b>还没有动作记录</b><p>可以直接在上方写一句观察理由，后端会写入 /api/journal/note。</p></div>';
      return;
    }
    grid.innerHTML = items.map((item) => `
      <div class="component-card journal-card">
        <span>${fmtDateTime(item.created_ts || item.ts || 0)}</span>
        <b>${candidateActionText(item.action, "BUY")}</b>
        <small>${escapeHtml(item.market_slug || shortWallet(item.wallet) || "-")}</small>
        <p>${escapeHtml(String(item.rationale || item.text || "无备注"))}</p>
      </div>
    `).join("");
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

  function computeOpsGate(data, monitor30m, monitor12h, eodReport, now) {
    const startup = data && data.startup && typeof data.startup === "object" ? data.startup : {};
    const reconciliation = data && data.reconciliation && typeof data.reconciliation === "object" ? data.reconciliation : {};
    const eod = eodReport && typeof eodReport === "object" ? eodReport : EMPTY_RECONCILIATION_EOD_REPORT;
    const report30 = monitor30m && typeof monitor30m === "object" ? monitor30m : EMPTY_MONITOR_REPORT("monitor_30m");
    const report12 = monitor12h && typeof monitor12h === "object" ? monitor12h : EMPTY_MONITOR_REPORT("monitor_12h");
    const nowTs = Number(now || data && data.ts || Math.floor(Date.now() / 1000));
    const checks = Array.isArray(startup.checks) ? startup.checks : [];
    const reconciliationIssues = Array.isArray(reconciliation.issues) ? reconciliation.issues.filter((item) => String(item || "").trim()) : [];
    const eodIssues = Array.isArray(eod.issues) ? eod.issues : [];
    const reconciliationStatus = String(reconciliation.status || "").toLowerCase();
    const eodStatus = String(eod.status || "").toLowerCase();
    const report30FinalKind = recommendationKind(report30.final_recommendation);
    const report12FinalKind = recommendationKind(report12.final_recommendation);
    const report30RawKind = recommendationKind(report30.recommendation);
    const report12RawKind = recommendationKind(report12.recommendation);
    const report30Freshness = reportFreshnessMeta(report30, 30 * 60, nowTs);
    const report12Freshness = reportFreshnessMeta(report12, 12 * 60 * 60, nowTs);
    const report30GateKind = report30Freshness.stale ? "ready" : report30FinalKind;
    const report12GateKind = report12Freshness.stale ? "ready" : report12FinalKind;
    const pendingOrderCount = Math.max(
      Number(reconciliation.pending_orders || 0),
      Number(reconciliation.pending_entry_orders || 0) + Number(reconciliation.pending_exit_orders || 0),
      Number(eod.reconciliation && eod.reconciliation.pending_orders || 0)
    );
    const monitorFocus = [
      report30RawKind !== "ready" || report30Freshness.stale ? `30m ${monitorWindowDisplaySummary(report30, 30 * 60, nowTs)}` : "",
      report12RawKind !== "ready" || report12Freshness.stale ? `12h ${monitorWindowDisplaySummary(report12, 12 * 60 * 60, nowTs)}` : "",
    ].filter(Boolean);
    const staleMonitorWindows = [
      report30Freshness.stale ? `30m ${report30Freshness.ageLabel}` : "",
      report12Freshness.stale ? `12h ${report12Freshness.ageLabel}` : "",
    ].filter(Boolean);
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
    if (startup.ready === false || report30GateKind === "block" || report12GateKind === "block") {
      level = "block";
    } else if (reconciliationStatus === "fail" || eodStatus === "fail" || report30GateKind === "escalate" || report12GateKind === "escalate") {
      level = "escalate";
    } else if (
      reconciliationStatus === "warn"
      || eodStatus === "warn"
      || report30GateKind === "observe"
      || report12GateKind === "observe"
      || report30Freshness.stale
      || report12Freshness.stale
    ) {
      level = "observe";
    }

    let title = "执行门禁已就绪";
    let detail = "startup、自检、monitor 和 reconciliation 当前没有阻断项，可以继续观察策略与执行质量。";
    if (level === "block") {
      title = "运行门禁阻断";
      detail = String(report12.final_recommendation || report30.final_recommendation || "启动自检未通过，或 live 前置条件缺失。优先修复环境与账户问题。");
    } else if (level === "escalate") {
      title = "执行对账需要升级处理";
      if (reconciliationStatus === "fail" || eodStatus === "fail") {
        const summary = (reconciliationIssues.length > 0 ? reconciliationIssues : eodIssues).slice(0, 2).join("; ");
        detail = `reconciliation 当前为 FAIL${summary ? `（${summary}）` : ""}。先停参数变更并生成 EOD 对账报告核对账本漂移。`;
      } else {
        detail = String(report12.final_recommendation || report30.final_recommendation || "账本与 broker 事实层可能已经漂移，建议先停参数变更。");
      }
    } else if (level === "observe") {
      title = "运行中有警告，先观察再调参";
      if (reconciliationStatus === "warn" || eodStatus === "warn") {
        const summary = (reconciliationIssues.length > 0 ? reconciliationIssues : eodIssues).slice(0, 2).join("; ");
        detail = `reconciliation 当前为 WARN${summary ? `（${summary}）` : ""}。先核对对账与同步新鲜度，再判断 monitor 是否只是样本不足。`;
      } else if (monitorFocus.length > 0) {
        detail = `monitor 当前仍在观察：${monitorFocus.join(" / ")}。先补样本或刷新过期报告，再决定是否调参数。`;
      } else if (staleMonitorWindows.length > 0) {
        detail = `monitor 报告已过期：${staleMonitorWindows.join(" / ")}。先刷新 30m / 12h 报告，再判断是否真的需要调参。`;
      } else {
        detail = "当前更多像执行告警而不是策略信号结论。";
      }
    }

    const items = [
      {
        label: "启动自检",
        value: startup.ready === false ? `${Number(startup.failure_count || 0)} fail / ${Number(startup.warning_count || 0)} warn` : startup.ready === true ? "ready" : "unknown",
      },
      {
        label: "30m",
        value: monitorWindowDisplaySummary(report30, 30 * 60, nowTs),
      },
      {
        label: "12h",
        value: monitorWindowDisplaySummary(report12, 12 * 60 * 60, nowTs),
      },
      {
        label: "对账",
        value: reconciliationStatusSummary(reconciliation, eod),
      },
    ];

    const issueParts = [];
    for (const row of checks.filter((item) => {
      const status = String(item && item.status || "").toUpperCase();
      return status === "FAIL" || status === "WARN";
    }).slice(0, 4)) {
      const status = String(row && row.status || "");
      issueParts.push(`${String(row.name || "startup")}: ${String(row.message || status)}`);
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
        if (pendingOrderCount > 0) {
          pushAction(
            "wait",
            "确认 heartbeat 策略",
            `当前有 <code>${pendingOrderCount}</code> 个 pending orders，但 SDK 可能缺少 heartbeat 能力，先避免依赖长时间 resting orders；当前提示: ${message || name}.`,
            [
              { type: "open", label: "打开状态 JSON", value: "/api/state" },
              { type: "jump", label: "跳到诊断", value: "diagnostics-panel" },
            ]
          );
        }
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

    if (inconclusiveWindows.length > 0) {
      const windowSummary = inconclusiveWindows
        .map((item) => `${item.label} 连续 ${item.count > 0 ? item.count : "?"} 个`)
        .join(" / ");
      pushAction(
        "wait",
        "补足样本窗口",
        `当前 monitor 仍有 <code>${windowSummary}</code> INCONCLUSIVE 窗口，先继续观察 EXEC 样本，不要基于 0 样本调参数。`,
        [
          { type: "jump", label: "跳到诊断", value: "diagnostics-panel" },
          { type: "jump", label: "跳到监控面板", value: "monitor-report-panel" },
        ]
      );
    }

    if (report30RawKind !== "ready" || report12RawKind !== "ready" || report30Freshness.stale || report12Freshness.stale) {
      pushAction(
        level === "block" || level === "escalate" ? "danger" : "wait",
        "先处理 monitor 摘要里的执行问题",
        `优先阅读 30m / 12h monitor 的原始建议，并刷新过期窗口；当前 30m ${monitorWindowDisplaySummary(report30, 30 * 60, nowTs)} / 12h ${monitorWindowDisplaySummary(report12, 12 * 60 * 60, nowTs)}。`,
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
    const visibleActions = actions.slice(0, 5);
    checksEl.innerHTML = items.length > 0
      ? items.map((item) => `<li><span>${String(item.label || "-")}</span><b>${String(item.value || "-")}</b></li>`).join("")
      : '<li><span>状态</span><b>等待数据...</b></li>';
    if (issues.length > 0) {
      checksEl.innerHTML += issues.slice(0, 2).map((item) => `<li><span>重点问题</span><b>${String(item)}</b></li>`).join("");
    }

    actionsMetaEl.textContent = actions.length > visibleActions.length ? `${visibleActions.length}/${actions.length} actions` : `${actions.length} actions`;
    actionsEl.innerHTML = visibleActions.length > 0
      ? visibleActions.map((item) => {
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
    const report30Freshness = reportFreshnessMeta(report30, 30 * 60, now);
    const report12Freshness = reportFreshnessMeta(report12, 12 * 60 * 60, now);
    const eodFreshness = reportFreshnessMeta(eod, 24 * 60 * 60, now);
    const internalDiff = Number(eodReconciliation.internal_vs_ledger_diff || 0);
    const fillCount = Number(ledgerSummary.fill_count || 0);
    metaEl.textContent = [
      `30m ${report30Freshness.ageLabel}${report30Freshness.stale ? " · STALE" : ""}`,
      `12h ${report12Freshness.ageLabel}${report12Freshness.stale ? " · STALE" : ""}`,
      `EOD ${eodFreshness.ageLabel}`,
    ].join(" · ");

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
    let calloutBody = `30m ${report30Freshness.ageLabel}${report30Freshness.stale ? " · STALE" : ""}，12h ${report12Freshness.ageLabel}${report12Freshness.stale ? " · STALE" : ""}，EOD ${eodFreshness.ageLabel}。`;
    if (startupReady === false) {
      calloutCls = "danger";
      calloutTag = "阻断";
      calloutTitle = "启动自检未就绪";
      calloutBody = "先修复 smoke、账户或 broker 前置条件，再讨论参数或策略表现。";
    } else if (String(eod.status || "").toLowerCase() === "fail" || String(report12.reconciliation_status || "").toLowerCase() === "fail" || String(report30.reconciliation_status || "").toLowerCase() === "fail") {
      calloutCls = "danger";
      calloutTag = "对账失败";
      calloutTitle = "执行事实层存在漂移";
      calloutBody = String(eodIssues[0] || report12.final_recommendation || report30.final_recommendation || recommendations[0] || "请优先检查 ledger 漂移、broker 同步和陈旧 pending 单。");
    } else if (String(eod.status || "").toLowerCase() === "warn" || String(report12.reconciliation_status || "").toLowerCase() === "warn" || String(report30.reconciliation_status || "").toLowerCase() === "warn" || report12Freshness.stale || report30Freshness.stale) {
      calloutCls = "wait";
      calloutTag = "观察";
      calloutTitle = "执行层有警告，暂不适合调参数";
      calloutBody = String(
        report12Freshness.stale || report30Freshness.stale
          ? `monitor 报告存在过期窗口：30m ${report30Freshness.ageLabel}${report30Freshness.stale ? " · STALE" : ""}，12h ${report12Freshness.ageLabel}${report12Freshness.stale ? " · STALE" : ""}。`
          : eodIssues[0] || report12.final_recommendation || report30.final_recommendation || recommendations[0] || "先处理 stale pending orders、snapshot age 和 reconcile age。"
      );
    }
    calloutEl.innerHTML = `<span class="tag ${calloutCls}">${calloutTag}</span><div><b>${calloutTitle}</b><p>${calloutBody}</p></div>`;

    const renderMonitorWindow = (el, report, label, windowSeconds) => {
      const counts = report.counts && typeof report.counts === "object" ? report.counts : {};
      const ratios = report.ratios && typeof report.ratios === "object" ? report.ratios : {};
      const reconciliation = report.reconciliation && typeof report.reconciliation === "object" ? report.reconciliation : {};
      const [sampleCls, sampleTag] = reportStatusMeta(report.sample_status || "unknown");
      const [decisionCls, decisionTag] = reportDecisionMeta(report.final_recommendation || report.recommendation, report.reconciliation_status || reconciliation.status || "unknown");
      const [reconCls, reconTag] = reportStatusMeta(report.reconciliation_status || reconciliation.status || "unknown");
      const freshness = reportFreshnessMeta(report, windowSeconds, now);
      const [freshCls, freshTag] = freshness.stale ? ["danger", `STALE · ${freshness.ageLabel}`] : ["wait", freshness.ageLabel];
      const rawRecommendation = String(report.recommendation || "").trim();
      const finalRecommendation = String(report.final_recommendation || "").trim();
      const recommendationText = finalRecommendation && rawRecommendation && finalRecommendation !== rawRecommendation
        ? `${finalRecommendation} · 原始 monitor: ${rawRecommendation}`
        : finalRecommendation || rawRecommendation || "";
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
        `<li><div class="review-main"><span>${label} 窗口</span><b><span class="tag ${sampleCls}">${sampleTag}</span> <span class="tag ${decisionCls}">${decisionTag}</span> <span class="tag ${freshCls}">${freshTag}</span></b></div><div class="review-sub">${generated > 0 ? `${fmtDateTime(generated)} · ${historyAgeLabel(generated, now)}` : "尚未生成报告"}</div></li>`,
        `<li><div class="review-main"><span>样本与计数</span><b>${exec} EXEC</b></div><div class="review-sub">skip max ${skipMax} · cooldown ${addCd} · time exit ${timeExit}${reject > 0 ? ` · reject ${reject}` : ""}</div></li>`,
        `<li><div class="review-main"><span>比例与同步</span><b><span class="tag ${reconCls}">${reconTag}</span></b></div><div class="review-sub">skip ${skipRatio} · exit ${exitRatio}${reject > 0 || ratios.reject_wallet_failures_per_exec != null ? ` · reject ${rejectRatio}` : ""} · reconcile ${recAge > 0 ? fmtAge(recAge) : "--"} · events ${eventAge > 0 ? fmtAge(eventAge) : "--"}</div></li>`,
        `<li><div class="review-main"><span>最终建议</span><b>${decisionTag}</b></div><div class="review-sub">${recommendationText || issueText || "暂无建议"}</div></li>`,
      ].join("");
    };

    renderMonitorWindow(list30El, report30, "30m", 30 * 60);
    renderMonitorWindow(list12El, report12, "12h", 12 * 60 * 60);

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
    const report30Freshness = reportFreshnessMeta(report30, 30 * 60, now);
    const report12Freshness = reportFreshnessMeta(report12, 12 * 60 * 60, now);
    const eodFreshness = reportFreshnessMeta(eod, 24 * 60 * 60, now);

    lastDiagnosticsState = state;
    lastDiagnosticsMonitor30 = report30;
    lastDiagnosticsMonitor12 = report12;
    lastDiagnosticsEod = eod;
    lastDiagnosticsNow = now;

    metaEl.textContent = [
      `startup ${Number(state.ts || 0) > 0 ? historyAgeLabel(state.ts, now) : "未记录"}`,
      `30m ${report30Freshness.ageLabel}${report30Freshness.stale ? " · STALE" : ""}`,
      `12h ${report12Freshness.ageLabel}${report12Freshness.stale ? " · STALE" : ""}`,
      `EOD ${eodFreshness.ageLabel}`,
    ].join(" · ");

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
    if (report30.final_recommendation || report30Freshness.stale) {
      pushIssue("30m monitor", monitorWindowDisplaySummary(report30, 30 * 60, now), report30Freshness.stale ? "warn" : (recommendationKind(report30.final_recommendation) === "ready" ? "ok" : recommendationKind(report30.final_recommendation)));
    }
    if (report12.final_recommendation || report12Freshness.stale) {
      pushIssue("12h monitor", monitorWindowDisplaySummary(report12, 12 * 60 * 60, now), report12Freshness.stale ? "warn" : (recommendationKind(report12.final_recommendation) === "ready" ? "ok" : recommendationKind(report12.final_recommendation)));
    }
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

  function bindDecisionConsoleActions() {
    const workspaceNav = $("workspace-nav");
    if (workspaceNav && workspaceNav.dataset.bound !== "1") {
      workspaceNav.dataset.bound = "1";
      workspaceNav.addEventListener("click", (event) => {
        const button = event.target.closest("[data-workspace-view]");
        if (!button) return;
        const next = String(button.getAttribute("data-workspace-view") || "").trim();
        if (!next || next === workspaceView) return;
        workspaceView = next;
        persistExitReviewUiState();
        renderWorkspaceShell();
      });
    }

    const focusSwitch = $("candidate-focus-view-switch");
    if (focusSwitch && focusSwitch.dataset.bound !== "1") {
      focusSwitch.dataset.bound = "1";
      focusSwitch.addEventListener("click", (event) => {
        const button = event.target.closest("[data-candidate-focus-view]");
        if (!button) return;
        const next = String(button.getAttribute("data-candidate-focus-view") || "").trim();
        if (!next || next === candidateFocusView) return;
        candidateFocusView = next;
        persistExitReviewUiState();
        renderCandidateFocusViewSwitch();
        if ($("candidate-grid-meta")) {
          const visiblePayload = filteredCandidatePayload(lastDecisionCandidates || EMPTY_CANDIDATES).visiblePayload;
          const visibleCount = Array.isArray(visiblePayload.items) ? visiblePayload.items.length : 0;
          const totalCount = Array.isArray((lastDecisionCandidates && lastDecisionCandidates.items) || []) ? lastDecisionCandidates.items.length : 0;
          $("candidate-grid-meta").textContent = `${visibleCount}/${totalCount} visible · ${candidateFocusViewLabel(candidateFocusView)}`;
        }
      });
    }

    const switchEl = $("decision-mode-switch");
    if (switchEl && switchEl.dataset.bound !== "1") {
      switchEl.dataset.bound = "1";
      switchEl.addEventListener("click", async (event) => {
        const button = event.target.closest("[data-mode]");
        if (!button) return;
        const mode = String(button.getAttribute("data-mode") || "").trim();
        if (!mode || mode === controlState.decision_mode) return;
        decisionConsoleNotice = { cls: "wait", text: `切换 decision mode 到 ${modeLabel(mode)}...` };
        renderDecisionConsole(lastDecisionCandidates, lastDecisionMode, lastDecisionApiState);
        setButtonBusy(button, true);
        try {
          const next = await pushMode(mode);
          lastDecisionMode = {
            ...lastDecisionMode,
            ...(next && typeof next === "object" ? next : {}),
          };
          decisionConsoleNotice = { cls: "ok", text: `decision mode 已切换为 ${modeLabel(mode)}` };
          renderDecisionConsole(lastDecisionCandidates, lastDecisionMode, lastDecisionApiState);
          await refresh();
        } catch (_err) {
          decisionConsoleNotice = { cls: "danger", text: `decision mode 切换失败: ${modeLabel(mode)}` };
          renderDecisionConsole(lastDecisionCandidates, lastDecisionMode, lastDecisionApiState);
        } finally {
          setButtonBusy(button, false);
        }
      });
    }

    const candidatePanel = $("candidate-panel");
    if (candidatePanel && candidatePanel.dataset.filterBound !== "1") {
      candidatePanel.dataset.filterBound = "1";
      candidatePanel.addEventListener("click", (event) => {
        const filterButton = event.target.closest("[data-candidate-filter]");
        if (filterButton) {
          const key = String(filterButton.getAttribute("data-candidate-filter") || "").trim();
          const value = String(filterButton.getAttribute("data-filter-value") || "all").trim();
          if (key) updateCandidateQueueFilter({ [key]: value });
          return;
        }
        const clearButton = event.target.closest("#candidate-clear-filters");
        if (clearButton) {
          resetCandidateQueueFilter();
          return;
        }
        const toggleButton = event.target.closest("#candidate-toggle-filters");
        if (toggleButton) {
          candidateFiltersExpanded = !candidateFiltersExpanded;
          persistExitReviewUiState();
          renderCandidateFilterToggle();
        }
      });
      candidatePanel.addEventListener("input", (event) => {
        const searchInput = event.target.closest("#candidate-search-input");
        if (!searchInput) return;
        updateCandidateQueueFilter({ search: String(searchInput.value || "") });
      });
      candidatePanel.addEventListener("change", (event) => {
        const sortSelect = event.target.closest("#candidate-sort-select");
        if (!sortSelect) return;
        updateCandidateQueueFilter({ sort: String(sortSelect.value || "score_desc") });
      });
    }

    const grid = $("candidate-grid");
    if (grid && grid.dataset.bound !== "1") {
      grid.dataset.bound = "1";
      grid.addEventListener("click", async (event) => {
        const button = event.target.closest("[data-action]");
        const card = button ? button.closest("[data-candidate-id]") : event.target.closest("[data-candidate-id]");
        const candidateId = card ? String(card.getAttribute("data-candidate-id") || "").trim() : "";
        const candidateSide = card ? String(card.getAttribute("data-candidate-side") || "BUY").trim().toUpperCase() : "BUY";
        if (!candidateId) return;
        if (!button) {
          if (candidateId === selectedCandidateId) return;
          selectedCandidateId = candidateId;
          candidateFocusView = "summary";
          persistExitReviewUiState();
          lastCandidateDetailApiState = {
            ok: lastCandidateDetailApiState.ok,
            error: "",
            candidateId,
            pending: true,
          };
          renderCandidateFocusViewSwitch();
          renderDecisionConsole(lastDecisionCandidates, lastDecisionMode, lastDecisionApiState);
          loadCandidateDetail(candidateId, { force: true })
            .catch(() => null)
            .finally(() => {
              renderDecisionConsole(lastDecisionCandidates, lastDecisionMode, lastDecisionApiState);
            });
          return;
        }
        const action = String(button.getAttribute("data-action") || "").trim();
        if (!action) return;
        candidateRequestState[candidateId] = {
          kind: "pending",
          message: `${candidateActionText(action, candidateSide)} 提交中...`,
        };
        renderDecisionConsole(lastDecisionCandidates, lastDecisionMode, lastDecisionApiState);
        setButtonBusy(button, true);
        try {
          await pushCandidateAction(candidateId, action, "");
          candidateRequestState[candidateId] = {
            kind: "success",
            message: `${candidateActionText(action, candidateSide)} 已提交`,
          };
          renderDecisionConsole(lastDecisionCandidates, lastDecisionMode, lastDecisionApiState);
          await refresh();
        } catch (_err) {
          candidateRequestState[candidateId] = {
            kind: "error",
            message: `${candidateActionText(action, candidateSide)} 提交失败`,
          };
          renderDecisionConsole(lastDecisionCandidates, lastDecisionMode, lastDecisionApiState);
        } finally {
          setButtonBusy(button, false);
        }
      });
    }
  }

  function bindWalletProfileEditors() {
    const body = $("wallet-profiles-body");
    if (!body || body.dataset.bound === "1") return;
    body.dataset.bound = "1";

    const syncDraft = (target) => {
      const field = String(target.getAttribute("data-wallet-profile-field") || "").trim();
      const wallet = readAttrToken(target.getAttribute("data-wallet-profile-key") || "");
      const source = walletProfileSource(wallet);
      if (!field || !wallet || !source) return;
      const draft = walletProfileDraft(source);
      draft[field] = field === "enabled" ? !!target.checked : String(target.value || "");
      walletProfileDrafts[wallet] = draft;
    };

    body.addEventListener("input", (event) => {
      const input = event.target.closest("[data-wallet-profile-field][data-wallet-profile-key]");
      if (!input) return;
      syncDraft(input);
    });
    body.addEventListener("change", (event) => {
      const input = event.target.closest("[data-wallet-profile-field][data-wallet-profile-key]");
      if (!input) return;
      syncDraft(input);
      renderWalletProfilesPanel(lastWalletProfiles, lastWalletProfilesApiState);
    });
    body.addEventListener("click", async (event) => {
      const button = event.target.closest("button[data-wallet-profile-save]");
      if (!button) return;
      const wallet = readAttrToken(button.getAttribute("data-wallet-profile-save") || "");
      const source = walletProfileSource(wallet);
      if (!wallet || !source) return;
      const draft = walletProfileDraft(source);
      if (!walletProfileChanged(source, draft)) {
        walletProfileRequestState[wallet] = { kind: "success", message: "无改动" };
        renderWalletProfilesPanel(lastWalletProfiles, lastWalletProfilesApiState);
        return;
      }
      walletProfileRequestState[wallet] = { kind: "pending", message: "保存中..." };
      renderWalletProfilesPanel(lastWalletProfiles, lastWalletProfilesApiState);
      try {
        await postJson("/api/wallet-profiles/update", {
          wallet,
          tag: String(draft.tag || "").trim(),
          notes: String(draft.notes || "").trim(),
          enabled: !!draft.enabled,
          trust_score: Number(source.trust_score || 0),
          followability_score: Number(source.followability_score || 0),
          avg_hold_minutes: source.avg_hold_minutes,
          category: String(source.category || ""),
        });
        walletProfileDrafts[wallet] = {
          tag: String(draft.tag || "").trim(),
          notes: String(draft.notes || "").trim(),
          enabled: !!draft.enabled,
        };
        walletProfileRequestState[wallet] = { kind: "success", message: "已保存" };
        renderWalletProfilesPanel(lastWalletProfiles, lastWalletProfilesApiState);
        await refresh();
      } catch (_err) {
        walletProfileRequestState[wallet] = { kind: "error", message: "保存失败" };
        renderWalletProfilesPanel(lastWalletProfiles, lastWalletProfilesApiState);
      }
    });
  }

  function bindJournalComposer() {
    const input = $("journal-note-input");
    const button = $("journal-note-save");
    if (!input || !button || button.dataset.bound === "1") return;
    button.dataset.bound = "1";

    const submit = async () => {
      const text = String(input.value || "").trim();
      if (!text) {
        journalComposerNotice = { cls: "danger", text: "请输入一句理由后再提交" };
        renderJournalPanel(lastJournalSummary, lastJournalApiState);
        return;
      }
      journalComposerNotice = { cls: "wait", text: "日记提交中..." };
      renderJournalPanel(lastJournalSummary, lastJournalApiState);
      setButtonBusy(button, true);
      try {
        await postJson("/api/journal/note", {
          text,
          action: "note",
          wallet: selectedWallet || "",
          trace_id: selectedTraceId || "",
          result_tag: "manual_note",
        });
        input.value = "";
        journalComposerNotice = { cls: "ok", text: "已写入交易日记" };
        renderJournalPanel(lastJournalSummary, lastJournalApiState);
        await refresh();
      } catch (_err) {
        journalComposerNotice = { cls: "danger", text: "写入日记失败" };
        renderJournalPanel(lastJournalSummary, lastJournalApiState);
      } finally {
        setButtonBusy(button, false);
      }
    };

    button.addEventListener("click", submit);
    input.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" || event.shiftKey) return;
      event.preventDefault();
      submit();
    });
  }

  async function refresh() {
    try {
      const [data, monitor30m, monitor12h, reconciliationEod, candidateResult, modeResult, journalResult, walletProfilesResult] = await Promise.all([
        fetchJson("/api/state", null),
        fetchJson("/api/monitor/30m", EMPTY_MONITOR_REPORT("monitor_30m")),
        fetchJson("/api/monitor/12h", EMPTY_MONITOR_REPORT("monitor_12h")),
        fetchJson("/api/reconciliation/eod", EMPTY_RECONCILIATION_EOD_REPORT),
        fetchJsonState("/api/candidates", EMPTY_CANDIDATES),
        fetchJsonState("/api/mode", EMPTY_DECISION_MODE),
        fetchJsonState("/api/journal", EMPTY_JOURNAL),
        fetchJsonState("/api/wallet-profiles", EMPTY_WALLET_PROFILES),
      ]);
      const candidateData = candidateResult && candidateResult.data ? candidateResult.data : EMPTY_CANDIDATES;
      const modeData = modeResult && modeResult.data ? modeResult.data : EMPTY_DECISION_MODE;
      const journalData = journalResult && journalResult.data ? journalResult.data : EMPTY_JOURNAL;
      const walletProfilesData = walletProfilesResult && walletProfilesResult.data ? walletProfilesResult.data : EMPTY_WALLET_PROFILES;
      const stateData = data || {};
      renderWorkspaceShell();
      renderCandidateFocusViewSwitch();
      const visibleCandidatePayload = filteredCandidatePayload(candidateData || EMPTY_CANDIDATES).visiblePayload;
      const activeCandidate = selectedCandidate(visibleCandidatePayload) || null;
      await loadCandidateDetail(candidateKey(activeCandidate || ""), {
        force: candidateKey(activeCandidate || "") !== String(lastCandidateDetailApiState.candidateId || ""),
      });
      if (decisionConsoleNotice.cls === "ok" && candidateResult.ok && modeResult.ok) {
        decisionConsoleNotice = { cls: "", text: "" };
      }
      renderDecisionConsole(candidateData || (data && data.candidates) || EMPTY_CANDIDATES, modeData || (data && data.decision_mode) || EMPTY_DECISION_MODE, {
        candidates: { ok: !!(candidateResult && candidateResult.ok), error: String(candidateResult && candidateResult.error || "") },
        mode: { ok: !!(modeResult && modeResult.ok), error: String(modeResult && modeResult.error || "") },
      });
      renderWalletProfilesPanel(walletProfilesData || (data && data.wallet_profiles) || EMPTY_WALLET_PROFILES, {
        ok: !!(walletProfilesResult && walletProfilesResult.ok),
        error: String(walletProfilesResult && walletProfilesResult.error || ""),
      });
      renderJournalPanel(journalData || (data && data.journal_summary) || EMPTY_JOURNAL, {
        ok: !!(journalResult && journalResult.ok),
        error: String(journalResult && journalResult.error || ""),
      });
      renderNotifierPanel(stateData.notifier || EMPTY_NOTIFIER);
      const now = Number(stateData.ts || 0);
      renderArchivePanel(stateData, monitor30m, monitor12h, reconciliationEod, now);
      if (!data) return;

      const summary = stateData.summary || {};
      const account = stateData.account || {};
      const config = stateData.config || {};
      const control = stateData.control || {};
      const pollSec = Number(config.poll_interval_seconds || 0);
      const stateAgeSec = Math.max(0, Math.floor(Date.now() / 1000 - now));
      const slotUtil = Number(summary.slot_utilization_pct || summary.exposure_pct || 0);
      const mode = String(config.execution_mode || (config.dry_run ? "paper" : "live")).toLowerCase();
      const brokerName = String(config.broker_name || (mode === "live" ? "LiveClobBroker" : "PaperBroker"));
      const accountEquity = Number(account.equity_usd || stateData.account_equity || summary.equity || 0);
      const cashBalance = Number(account.cash_balance_usd || stateData.cash_balance_usd || summary.cash_balance_usd || 0);
      const positionsValue = Number(account.positions_value_usd || stateData.positions_value_usd || summary.positions_value_usd || 0);
      const trackedNotional = Number(account.tracked_notional_usd || stateData.tracked_notional_usd || summary.tracked_notional_usd || 0);
      const availableNotional = Number(account.available_notional_usd || stateData.available_notional_usd || summary.available_notional_usd || config.bankroll_usd || 0);
      const accountSnapshotTs = Number(account.account_snapshot_ts || stateData.account_snapshot_ts || summary.account_snapshot_ts || 0);
      const accountSnapshotLabel = accountSnapshotTs > 0 ? `${hhmm(accountSnapshotTs)} · ${historyAgeLabel(accountSnapshotTs, now)}` : "等待账户同步";
      const opsGate = computeOpsGate(data, monitor30m, monitor12h, reconciliationEod, now);
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
        $("budget-meta").textContent = `权益 ${accountEquity.toFixed(2)}U · 现金 ${cashBalance.toFixed(2)}U · 已用 ${trackedNotional.toFixed(2)}U / 可用 ${availableNotional.toFixed(2)}U`;
      }
      if ($("budget-fill")) {
        $("budget-fill").style.width = `${Math.max(0, Math.min(100, slotUtil)).toFixed(1)}%`;
      }

      if ($("kpi-mode")) $("kpi-mode").textContent = mode.toUpperCase();
      if ($("kpi-mode-note")) {
        $("kpi-mode-note").textContent = mode === "live" ? `${brokerName} · 会发送真实订单` : `${brokerName} · 不会发送真实订单`;
      }
      if ($("kpi-equity")) $("kpi-equity").textContent = fmtUsd(accountEquity, false);
      if ($("kpi-equity-note")) {
        $("kpi-equity-note").textContent = `现金 ${fmtUsd(cashBalance, false)} · 仓位 ${fmtUsd(positionsValue, false)}`;
      }
      if ($("kpi-cash")) $("kpi-cash").textContent = fmtUsd(cashBalance, false);
      if ($("kpi-cash-note")) {
        $("kpi-cash-note").textContent = `可用预算 ${availableNotional.toFixed(2)}U · ${accountSnapshotLabel}`;
      }
      if ($("kpi-pos")) $("kpi-pos").textContent = `${Number(summary.open_positions || 0)} / ${Number(summary.max_open_positions || 0)}`;
      if ($("kpi-slot-note")) $("kpi-slot-note").textContent = `槽位利用率 ${fmtPct(slotUtil, 2)}`;
      if ($("kpi-notional")) $("kpi-notional").textContent = fmtUsd(trackedNotional, false);
      if ($("kpi-notional-note")) {
        $("kpi-notional-note").textContent = `每轮信号 ${Number(summary.signals || 0)} · 账本快照 ${accountSnapshotLabel}`;
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
  renderWorkspaceShell();
  renderCandidateFocusViewSwitch();
  bindControlActions();
  bindDecisionConsoleActions();
  bindWalletProfileEditors();
  bindJournalComposer();
  bindArchiveActions();
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
