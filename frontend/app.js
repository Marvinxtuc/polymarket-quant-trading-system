(function () {
  const $ = (id) => document.getElementById(id);
  const i18n = window.PolyI18n || null;
  const t = (key, vars = {}, fallback = "") => {
    if (i18n && typeof i18n.t === "function") {
      return i18n.t(key, vars, fallback);
    }
    if (!fallback) return key;
    return String(fallback).replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_match, name) => String(vars && vars[name] != null ? vars[name] : ""));
  };
  const te = (group, value, fallback = "") => {
    if (i18n && typeof i18n.te === "function") {
      return i18n.te(group, value, fallback);
    }
    return fallback || String(value || "");
  };
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
    status_label: "",
    issues: [],
    issue_labels: [],
    startup: {},
    reconciliation: {},
    state_summary: {},
    ledger_summary: {},
    recommendation_codes: [],
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
    observability: {
      updated_ts: 0,
      candidate_count: 0,
      market_metadata: {
        hits: 0,
        misses: 0,
        coverage_pct: 0,
      },
      market_time_source: {
        metadata: 0,
        slug_legacy: 0,
        unknown: 0,
      },
      skip_reasons: {},
      recent_cycles: {
        cycles: 0,
        signals: 0,
        precheck_skipped: 0,
        market_time_source: {
          metadata: 0,
          slug_legacy: 0,
          unknown: 0,
        },
        skip_reasons: {},
      },
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
  const EMPTY_BLOCKBEATS = {
    updated_ts: 0,
    status: "unknown",
    stale_after_seconds: 180,
    prediction: {
      source: "disabled",
      status: "disabled",
      message: "",
      items: [],
    },
    important: {
      source: "disabled",
      status: "disabled",
      message: "",
      items: [],
    },
    errors: [],
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
  let lastBlockbeats = EMPTY_BLOCKBEATS;
  let lastBlockbeatsApiState = { ok: true, error: "" };
  let lastJournalSummary = EMPTY_JOURNAL;
  let lastJournalApiState = { ok: true, error: "" };
  const candidateRequestState = Object.create(null);
  const walletProfileDrafts = Object.create(null);
  const walletProfileRequestState = Object.create(null);
  let decisionConsoleNotice = { cls: "", text: "" };
  let journalComposerNotice = { cls: "", text: "" };

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
    return te("workspace_view", key || "overview", t("nav.workspace.overview"));
  }

  function candidateFocusViewLabel(value) {
    const key = String(value || "").trim().toLowerCase() === "detail" ? "detail" : "summary";
    return te("candidate_focus_view", key, key);
  }

  function renderWorkspaceShell() {
    const root = document.querySelector(".console");
    if (root) root.dataset.workspaceView = workspaceView;
    const metaEl = $("workspace-meta");
    if (metaEl) {
      const copy = {
        overview: t("workspace.meta.overview"),
        wallets: t("workspace.meta.wallets"),
        ops: t("workspace.meta.ops"),
        review: t("workspace.meta.review"),
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
      toggleBtn.textContent = candidateFiltersExpanded
        ? t("candidate.panel.toggleFiltersCollapse")
        : t("candidate.panel.toggleFiltersExpand");
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

  function translatedTextFromKey(meta, fallbackText = "") {
    if (!meta || typeof meta !== "object") return fallbackText;
    const key = String(meta.key || meta.message_key || "").trim();
    const vars = meta.vars && typeof meta.vars === "object" ? meta.vars : meta.message_params && typeof meta.message_params === "object" ? meta.message_params : {};
    if (key) return t(key, vars, fallbackText);
    return String(meta.text || meta.message || fallbackText || "");
  }

  function translateKnownPhrase(value) {
    const raw = String(value || "").trim();
    const normalized = raw.toLowerCase();
    if (!raw) return "";
    if (normalized === "reconciliation protect") return t("notifier.phrase.reconciliationProtect");
    if (normalized === "startup gate blocked") return t("notifier.phrase.startupGateBlocked");
    if (normalized === "is blocking new buy") return t("notifier.phrase.isBlockingNewBuy");
    if (normalized === "no body") return t("notifier.phrase.noBody");
    if (normalized === "chat unavailable") return t("notifier.phrase.chatUnavailable");
    return raw;
  }

  function humanizeIdentifier(value) {
    return String(value || "")
      .trim()
      .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
      .replace(/[_-]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function translateRegisteredLabel(prefix, name, normalize) {
    const raw = String(name || "").trim();
    if (!raw) return "";
    const normalized = typeof normalize === "function" ? normalize(raw) : raw.toLowerCase();
    const key = `${prefix}.${normalized}`;
    const translated = t(key);
    return translated !== key ? translated : humanizeIdentifier(raw);
  }

  function normalizeMetricName(name) {
    return String(name || "")
      .trim()
      .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
      .replace(/[\s-]+/g, "_")
      .toLowerCase();
  }

  function translateMetricLabel(name) {
    return translateRegisteredLabel("metric", name, normalizeMetricName);
  }

  function walletTierLabel(value) {
    return translateRegisteredLabel("enum.walletTier", value, normalizeMetricName);
  }

  function sideLabel(value) {
    return translateRegisteredLabel("enum.side", value, normalizeMetricName);
  }

  function translateStructuredText(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    const direct = translateKnownPhrase(raw);
    if (direct !== raw) return direct;
    if (!raw.includes("=") && !raw.includes("|")) return raw;
    return raw
      .split("|")
      .map((part) => String(part || "").trim())
      .filter(Boolean)
      .map((part) => {
        const match = part.match(/^([^=]+)=(.*)$/);
        if (!match) return translateKnownPhrase(part);
        const metric = String(match[1] || "").trim();
        const metricValue = String(match[2] || "").trim();
        const translatedValue = metric === "status"
          ? te("report_status", metricValue, metricValue)
          : translateKnownPhrase(metricValue);
        return t("common.kvInline", {
          label: translateMetricLabel(metric),
          value: translatedValue || metricValue,
        });
      })
      .join(t("common.separator"));
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
    if (s < 60) return t("common.time.seconds", { count: s });
    if (s < 3600) return t("common.time.minutes", { count: Math.floor(s / 60) });
    if (s < 86400) return t("common.time.hours", { count: Math.floor(s / 3600) });
    const days = Math.floor(s / 86400);
    const hours = Math.floor((s % 86400) / 3600);
    return t("common.time.daysHours", { days, hours });
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

  function parseTimestamp(value) {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value > 1000000000000 ? Math.floor(value / 1000) : Math.floor(value);
    }
    const text = String(value || "").trim();
    if (!text) return 0;
    if (/^\d+$/.test(text)) {
      return text.length >= 13 ? Math.floor(Number(text.slice(0, 13)) / 1000) : Number(text);
    }
    const parsed = Date.parse(text);
    return Number.isFinite(parsed) ? Math.floor(parsed / 1000) : 0;
  }

  function stripHtmlTags(value) {
    return String(value ?? "")
      .replace(/<[^>]*>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function fmtHoldMinutes(minutes) {
    const mins = Math.max(0, Number(minutes || 0));
    if (mins < 60) return t("common.time.minutes", { count: mins });
    const hours = Math.floor(mins / 60);
    const rem = mins % 60;
    if (hours < 24) {
      if (rem > 0) {
        return t("common.time.hoursMinutes", { hours, minutes: rem });
      }
      return t("common.time.hours", { count: hours });
    }
    const days = Math.floor(hours / 24);
    const hourRem = hours % 24;
    if (hourRem > 0) {
      return t("common.time.daysHours", { days, hours: hourRem });
    }
    return t("common.time.daysHours", { days, hours: 0 });
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
    if (controlState.emergency_stop) return t("enum.controlState.emergency_stop");
    if (controlState.reduce_only) return t("enum.controlState.reduce_only");
    if (controlState.pause_opening) return t("enum.controlState.pause_opening");
    return t("enum.controlState.normal");
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
        label: t("wallet.historyProfile.missing.label"),
        detail: t("wallet.historyProfile.missing.detail"),
      };
    }
    if (closed < minClosed) {
      return {
        cls: "cancel",
        label: t("wallet.historyProfile.thin.label"),
        detail: t("wallet.historyProfile.thin.detail", { closed, minClosed }),
      };
    }
    if (closed >= strongClosed || resolved >= strongResolved) {
      return {
        cls: "ok",
        label: t("wallet.historyProfile.strong.label"),
        detail: t("wallet.historyProfile.strong.detail", { closed, resolved }),
      };
    }
    return {
      cls: "wait",
      label: t("wallet.historyProfile.usable.label"),
      detail: t("wallet.historyProfile.usable.detail", { closed, resolved }),
    };
  }

  function historyAgeLabel(ts, now) {
    const refreshTs = Number(ts || 0);
    if (refreshTs <= 0 || now <= 0) return t("common.notRecorded");
    return t("common.ageLabel", { age: fmtAge(Math.max(0, now - refreshTs)) });
  }

  function stripHtmlText(value) {
    const html = String(value ?? "");
    if (!html) return "";
    const node = document.createElement("div");
    node.innerHTML = html;
    return String(node.textContent || node.innerText || "").replace(/\s+/g, " ").trim();
  }

  function normalizeFlexibleTimestamp(value) {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value > 1000000000000 ? Math.floor(value / 1000) : Math.floor(value);
    }
    const raw = String(value || "").trim();
    if (!raw) return 0;
    if (/^\d+$/.test(raw)) {
      const numeric = Number(raw);
      return numeric > 1000000000000 ? Math.floor(numeric / 1000) : Math.floor(numeric);
    }
    const parsed = Date.parse(raw.replace(" ", "T"));
    if (Number.isFinite(parsed)) return Math.floor(parsed / 1000);
    return 0;
  }

  function blockbeatsFeedSourceLabel(source) {
    const value = String(source || "").trim().toLowerCase();
    return te("blockbeats_source", value || "unknown", value || t("common.unknown"));
  }

  function blockbeatsFeedMeta(feed) {
    const status = String(feed && feed.status || "").trim().toLowerCase();
    const source = String(feed && feed.source || "").trim().toLowerCase();
    if (status === "ok" && source === "pro") return ["ok", t("enum.blockbeatsSource.pro")];
    if (status === "degraded" || source === "public_fallback") return ["wait", t("enum.blockbeatsSource.public_fallback")];
    if (status === "error" || source === "error") return ["danger", t("enum.blockbeatsSource.error")];
    if (status === "disabled" || source === "disabled") return ["cancel", t("enum.blockbeatsSource.disabled")];
    return ["cancel", blockbeatsFeedSourceLabel(source)];
  }

  function blockbeatsTimeLabel(value, now) {
    const ts = normalizeFlexibleTimestamp(value);
    if (ts > 0) return `${fmtDateTime(ts)} · ${historyAgeLabel(ts, now)}`;
    const raw = String(value || "").trim();
    return raw || t("common.notRecorded");
  }

  function renderBlockbeatsFeedItems(feed, now, emptyText) {
    const items = Array.isArray(feed && feed.items) ? feed.items : [];
    const [sourceCls, sourceLabel] = blockbeatsFeedMeta(feed);
    if (items.length <= 0) {
      const message = translatedTextFromKey(feed, String(feed && feed.message || "").trim());
      return `<li><div class="review-main"><span>${escapeHtml(emptyText)}</span><b><span class="tag ${sourceCls}">${escapeHtml(sourceLabel)}</span></b></div><div class="review-sub">${escapeHtml(message || t("blockbeats.feed.noItems"))}</div></li>`;
    }
    return items.map((item) => {
      const title = String(item && item.title || "").trim() || t("blockbeats.feed.untitled");
      const url = String(item && item.url || "").trim();
      const content = stripHtmlText(item && item.content || "");
      const excerpt = content.length > 140 ? `${content.slice(0, 140)}...` : content;
      const tags = Array.isArray(item && item.tags) ? item.tags.filter((tag) => String(tag || "").trim()) : [];
      const timeLabel = blockbeatsTimeLabel(item && item.create_time, now);
      const linkHtml = url
        ? `<a class="catalyst-link" href="${escapeHtml(url)}" target="_blank" rel="noreferrer noopener">${escapeHtml(title)}</a>`
        : `<span class="catalyst-link muted">${escapeHtml(title)}</span>`;
      return `<li>
        <div class="review-main">
          <span class="catalyst-headline">${linkHtml}</span>
          <b>${escapeHtml(timeLabel)}</b>
        </div>
        <div class="review-sub catalyst-detail">
          <span class="catalyst-source"><span class="tag ${sourceCls}">${escapeHtml(sourceLabel)}</span>${tags.length > 0 ? ` <span>${escapeHtml(tags.slice(0, 3).join(" / "))}</span>` : ""}</span>
        </div>
        ${excerpt ? `<div class="catalyst-copy">${escapeHtml(excerpt)}</div>` : ""}
      </li>`;
    }).join("");
  }

  function componentLabel(key) {
    return translateRegisteredLabel("wallet.component", key, (value) => String(value || "").trim().toLowerCase());
  }

  function sampleVerdict(sample) {
    if (sample && sample.resolved === true) {
      if (sample.resolved_correct === true) return ["ok", t("wallet.sampleVerdict.resolvedCorrect")];
      if (sample.resolved_correct === false) return ["danger", t("wallet.sampleVerdict.resolvedWrong")];
    }
    const pnl = Number(sample && sample.realized_pnl || 0);
    if (pnl > 0) return ["ok", t("wallet.sampleVerdict.profit")];
    if (pnl < 0) return ["cancel", t("wallet.sampleVerdict.loss")];
    return ["wait", t("wallet.sampleVerdict.flat")];
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
    if (value === "resonance_exit") return ["danger", fallbackLabel || t("enum.exitKind.resonance_exit")];
    if (value === "smart_wallet_exit") return ["wait", fallbackLabel || t("enum.exitKind.smart_wallet_exit")];
    if (value === "time_exit") return ["cancel", fallbackLabel || t("enum.exitKind.time_exit")];
    if (value === "emergency_exit") return ["danger", fallbackLabel || t("enum.exitKind.emergency_exit")];
    return ["ok", fallbackLabel || t("enum.exitKind.entry")];
  }

  function exitResultMeta(result, fallbackLabel = "") {
    const value = String(result || "").trim().toLowerCase();
    if (value === "emergency") return ["danger", fallbackLabel || t("enum.exitResult.emergency")];
    if (value === "full_exit") return ["ok", fallbackLabel || t("enum.exitResult.full_exit")];
    if (value === "partial_trim") return ["wait", fallbackLabel || t("enum.exitResult.partial_trim")];
    if (value === "reject") return ["danger", fallbackLabel || t("enum.exitResult.reject")];
    return ["cancel", fallbackLabel || t("enum.exitResult.unknown")];
  }

  function orderResultStatusMeta(status) {
    const value = String(status || "").trim().toUpperCase();
    if (value === "FILLED") return ["ok", t("orders.status.filled")];
    if (value === "REJECTED") return ["danger", t("orders.status.rejected")];
    if (value === "PENDING") return ["wait", t("orders.status.pending")];
    if (value === "CANCELED" || value === "CANCELLED") return ["cancel", t("orders.status.canceled")];
    if (value === "CLEARED") return ["cancel", t("orders.status.cleared")];
    return ["cancel", value || t("common.unknown")];
  }

  function reportStatusMeta(status) {
    const value = String(status || "").trim().toLowerCase();
    if (value === "ok" || value === "ready") return ["ok", te("report_status", value, t("common.status.ok"))];
    if (value === "warn" || value === "warning") return ["wait", te("report_status", value, t("common.status.warn"))];
    if (value === "fail" || value === "blocked" || value === "error") return ["danger", te("report_status", value, t("common.status.fail"))];
    if (value === "conclusive") return ["ok", te("report_status", value, t("common.status.ok"))];
    if (value === "inconclusive") return ["cancel", te("report_status", value, t("common.waiting"))];
    return ["cancel", te("report_status", value || "unknown", t("common.unknown"))];
  }

  function startupCheckMeta(status) {
    const value = String(status || "").trim().toUpperCase();
    if (value === "PASS") return ["ok", t("enum.startupCheck.PASS")];
    if (value === "WARN") return ["wait", t("enum.startupCheck.WARN")];
    if (value === "FAIL") return ["danger", t("enum.startupCheck.FAIL")];
    return ["cancel", t("enum.startupCheck.UNKNOWN")];
  }

  const STARTUP_CHECK_NAME_KEYS = {
    network_smoke: "diagnostics.startup.names.networkSmoke",
    api_credentials: "diagnostics.startup.names.apiCredentials",
    funder_address: "diagnostics.startup.names.funderAddress",
    signature_type: "diagnostics.startup.names.signatureType",
    market_preflight: "diagnostics.startup.names.marketPreflight",
    order_status_support: "diagnostics.startup.names.orderStatusSupport",
    heartbeat_support: "diagnostics.startup.names.heartbeatSupport",
    user_stream: "diagnostics.startup.names.userStream",
    clob_host: "diagnostics.startup.names.clobHost",
  };

  function startupCheckNameLabel(name) {
    const value = String(name || "").trim();
    if (!value) return t("diagnostics.startup.defaultName");
    const key = STARTUP_CHECK_NAME_KEYS[value];
    return key ? t(key) : humanizeIdentifier(value);
  }

  function reportDecisionMeta(text, fallbackStatus = "") {
    const value = String(text || "").trim();
    const upper = value.toUpperCase();
    if (upper.startsWith("BLOCK")) return ["danger", t("enum.decision.BLOCK")];
    if (upper.startsWith("ESCALATE")) return ["danger", t("enum.decision.ESCALATE")];
    if (upper.startsWith("OBSERVE")) return ["wait", t("enum.decision.OBSERVE")];
    if (upper.startsWith("NO ESCALATION")) return ["ok", t("enum.decision.OK")];
    if (upper.startsWith("CONSECUTIVE_INCONCLUSIVE")) return ["cancel", t("enum.decision.INCONCLUSIVE")];
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
      ageLabel: generatedTs > 0 ? historyAgeLabel(generatedTs, currentTs || generatedTs) : t("common.notGenerated"),
      stale,
    };
  }

  function inconclusiveWindowLabel(report) {
    const payload = report && typeof report === "object" ? report : {};
    const count = Number(payload.consecutive_inconclusive_windows || 0);
    return count > 0
      ? t("monitor.window.inconclusiveCount", { count })
      : t("enum.reportStatus.inconclusive");
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
    return sampleStatus ? te("report_status", sampleStatus.toLowerCase(), sampleStatus) : t("common.unknown");
  }

  function monitorWindowDisplaySummary(report, windowSeconds, now) {
    const summary = monitorWindowSummary(report);
    const freshness = reportFreshnessMeta(report, windowSeconds, now);
    return t("monitor.window.summaryWithFreshness", {
      summary,
      freshness: freshnessAgeLabel(freshness),
    });
  }

  function freshnessAgeLabel(freshness) {
    const safe = freshness && typeof freshness === "object" ? freshness : {};
    const age = String(safe.ageLabel || t("common.unknown"));
    return safe.stale ? t("monitor.window.staleAgeTag", { age }) : age;
  }

  function reconciliationStatusSummary(reconciliation, eodReport) {
    const live = reconciliation && typeof reconciliation === "object" ? reconciliation : {};
    const eod = eodReport && typeof eodReport === "object" ? eodReport : {};
    const status = String(live.status || eod.status || "unknown");
    const statusLabel = te("report_status", status, status);
    const liveIssues = Array.isArray(live.issues) ? live.issues.filter((item) => String(item || "").trim()).map((item) => issueDisplayText(item)) : [];
    const eodIssues = Array.isArray(eod.issue_labels) && eod.issue_labels.length > 0
      ? eod.issue_labels.filter((item) => String(item || "").trim())
      : Array.isArray(eod.issues)
        ? eod.issues.filter((item) => String(item || "").trim()).map((item) => issueDisplayText(item))
        : [];
    const issues = liveIssues.length > 0 ? liveIssues : eodIssues;
    return issues.length > 0 ? `${statusLabel} · ${String(issues[0])}` : statusLabel;
  }

  function modeLabel(mode) {
    const value = String(mode || "").trim().toLowerCase();
    return te("decision_mode", value || "manual", value || t("common.unknown"));
  }

  function candidateStatusMeta(status) {
    const value = String(status || "").trim().toLowerCase();
    if (value === "approved" || value === "queued") return ["wait", te("candidate_status", value, value)];
    if (value === "watched") return ["cancel", te("candidate_status", value, value)];
    if (value === "executed") return ["ok", te("candidate_status", value, value)];
    if (value === "submitted") return ["wait", te("candidate_status", value, value)];
    if (value === "rejected" || value === "risk_rejected") return ["danger", te("candidate_status", value, value)];
    if (value === "ignored") return ["cancel", te("candidate_status", value, value)];
    if (value === "expired") return ["cancel", te("candidate_status", value, value)];
    return ["wait", te("candidate_status", value || "pending", value || t("common.unknown"))];
  }

  function candidateReplayStatusMeta(status) {
    const value = String(status || "").trim().toLowerCase();
    if (value === "filled") return ["ok", t("candidate.panel.replay.status.filled")];
    if (value === "precheck_skipped") return ["wait", t("candidate.panel.replay.status.precheckSkipped")];
    if (value.includes("reject")) return ["danger", t("candidate.panel.replay.status.rejected")];
    return ["cancel", t("candidate.panel.replay.status.recentSignal")];
  }

  function candidateActionText(action, side) {
    const value = String(action || "").trim().toLowerCase();
    if (value === "follow" && String(side || "").trim().toUpperCase() === "SELL") {
      return t("enum.candidateAction.follow_sell");
    }
    return te("candidate_action", value || "default", value || t("enum.candidateAction.default"));
  }

  function channelDisplayName(channel) {
    const raw = String(channel || "").trim();
    return translateRegisteredLabel("common.channel", raw, normalizeMetricName) || t("common.channel.unavailable");
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
      { action: "watch", label: t("enum.candidateAction.watch"), cls: "subtle" },
      { action: "ignore", label: t("enum.candidateAction.ignore"), cls: "ghost" },
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
    const sortKey = {
      score_desc: "candidate.sort.score_desc",
      freshness_desc: "candidate.sort.freshness_desc",
      confidence_desc: "candidate.sort.confidence_desc",
      wallet_score_desc: "candidate.sort.wallet_score_desc",
      observed_notional_desc: "candidate.sort.observed_notional_desc",
    }[value || "score_desc"] || "candidate.sort.score_desc";
    return t(sortKey);
  }

  function candidateFilterLabel(kind, value) {
    const rawKind = String(kind || "").trim().toLowerCase();
    const rawValue = String(value || "all").trim().toLowerCase();
    if (rawValue === "all" || !rawValue) return t("candidate.filter.all");
    if (rawKind === "status") {
      return te("candidate_status", rawValue, rawValue);
    }
    if (rawKind === "action") {
      return candidateActionText(rawValue, "BUY");
    }
    if (rawKind === "side") {
      return sideLabel(rawValue);
    }
    return rawValue;
  }

  function candidateFilterSummary(filters, visibleCount, totalCount) {
    const search = String(filters && filters.search || "").trim();
    const status = String(filters && filters.status || "all").trim();
    const action = String(filters && filters.action || "all").trim();
    const side = String(filters && filters.side || "all").trim();
    const parts = [
      t("candidate.panel.summary.visible", { visible: visibleCount, total: totalCount }),
      t("common.kvInline", { label: t("candidate.filter.status"), value: candidateFilterLabel("status", status) }),
      t("common.kvInline", { label: t("candidate.filter.action"), value: candidateFilterLabel("action", action) }),
      t("common.kvInline", { label: t("candidate.filter.side"), value: candidateFilterLabel("side", side) }),
      t("common.kvInline", { label: t("candidate.panel.sortLabel"), value: candidateSortLabel(filters && filters.sort) }),
    ];
    if (search) parts.unshift(t("common.kvInline", { label: t("candidate.panel.searchLabel"), value: `"${search}"` }));
    return parts.join(t("common.separator"));
  }

  function notifierParseModeLabel(value) {
    const mode = String(value || "plain").trim().toLowerCase() || "plain";
    const key = {
      plain: "notifier.parseMode.plain",
      html: "notifier.parseMode.html",
      markdown: "notifier.parseMode.markdown",
    }[mode];
    return key ? t(key) : mode;
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

  function candidateObservability(payload) {
    if (payload && typeof payload === "object" && payload.observability && typeof payload.observability === "object") {
      return payload.observability;
    }
    return EMPTY_CANDIDATES.observability;
  }

  function topCandidateSkipReason(observability) {
    const skipReasons = observability && typeof observability.skip_reasons === "object" ? observability.skip_reasons : {};
    const entries = Object.entries(skipReasons)
      .filter((entry) => Number(entry[1] || 0) > 0)
      .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0) || String(a[0] || "").localeCompare(String(b[0] || "")));
    return entries.length > 0 ? entries[0] : null;
  }

  function topRecentCycleSkipReason(observability) {
    const recentCycles = observability && typeof observability.recent_cycles === "object" ? observability.recent_cycles : {};
    const skipReasons = recentCycles && typeof recentCycles.skip_reasons === "object" ? recentCycles.skip_reasons : {};
    const entries = Object.entries(skipReasons)
      .filter((entry) => Number(entry[1] || 0) > 0)
      .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0) || String(a[0] || "").localeCompare(String(b[0] || "")));
    return entries.length > 0 ? entries[0] : null;
  }

  function recentReviewCandidates(review, limit = 6) {
    const rows = [];
    const cycles = review && typeof review === "object" && Array.isArray(review.cycles) ? review.cycles : [];
    const seen = new Set();
    for (const cycle of cycles) {
      if (!cycle || typeof cycle !== "object") continue;
      const cycleId = String(cycle.cycle_id || "").trim();
      const cycleTs = Number(cycle.ts || 0);
      const candidates = Array.isArray(cycle.candidates) ? cycle.candidates : [];
      for (const raw of candidates) {
        if (!raw || typeof raw !== "object") continue;
        const snapshot = raw.candidate_snapshot && typeof raw.candidate_snapshot === "object" ? raw.candidate_snapshot : {};
        const decision = raw.decision_snapshot && typeof raw.decision_snapshot === "object" ? raw.decision_snapshot : {};
        const signalId = String(raw.signal_id || snapshot.signal_id || "").trim();
        const traceId = String(raw.trace_id || snapshot.trace_id || "").trim();
        const dedupeKey = signalId || traceId || `${cycleId}:${rows.length}`;
        if (seen.has(dedupeKey)) continue;
        seen.add(dedupeKey);
        rows.push({
          cycle_id: cycleId,
          cycle_ts: cycleTs,
          signal_id: signalId,
          trace_id: traceId,
          title: String(raw.title || snapshot.market_slug || raw.market_slug || raw.token_id || snapshot.token_id || "-"),
          market_slug: String(raw.market_slug || snapshot.market_slug || raw.title || "-"),
          token_id: String(raw.token_id || snapshot.token_id || ""),
          outcome: String(raw.outcome || snapshot.outcome || ""),
          wallet: String(raw.wallet || snapshot.wallet || ""),
          side: String(raw.side || snapshot.side || "BUY").toUpperCase(),
          wallet_score: Number(raw.wallet_score || snapshot.wallet_score || 0),
          wallet_tier: String(raw.wallet_tier || snapshot.wallet_tier || ""),
          action_label: String(raw.action_label || snapshot.position_action_label || ""),
          final_status: String(raw.final_status || "candidate"),
          skip_reason: String(decision.skip_reason || ""),
          market_time_source: String(decision.market_time_source || ""),
          market_metadata_hit: !!decision.market_metadata_hit,
          decision_reason: String(raw.decision_reason || decision.risk_reason || ""),
        });
        if (rows.length >= limit) return rows;
      }
    }
    return rows;
  }

  function candidateReviewTrail(candidate, side) {
    return buildCandidateReviewTrail(candidate, side);
  }

  function candidateWalletCountText(count) {
    const total = Math.max(0, Number(count || 0));
    return t("candidate.shared.walletCount", { count: total });
  }

  function candidateExplainKind(candidate) {
    const status = String(candidateField(candidate, "status") || "").trim().toLowerCase();
    const suggested = String(candidateField(candidate, "suggested_action") || "").trim().toLowerCase();
    const gateState = candidateGateState(candidate, candidateField(candidate, "side") || candidate.side || "BUY");
    if (status === "ignored" || status === "rejected" || suggested === "ignore") return "ignore";
    if (gateState.gated) return gateState.score >= 70 ? "blockedHighScore" : "blocked";
    if (status === "watched" || suggested === "watch") return "watch";
    return "recommend";
  }

  function candidateExplainTone(kind) {
    const value = String(kind || "").trim();
    if (value === "recommend") return "positive";
    if (value === "watch" || value === "blockedHighScore") return "warn";
    if (value === "blocked" || value === "ignore") return "danger";
    return "warn";
  }

  function candidateExplainTitle(input) {
    const kind = typeof input === "string" ? input : candidateExplainKind(input);
    const key = {
      recommend: "candidate.explain.title.recommend",
      watch: "candidate.explain.title.watch",
      blocked: "candidate.explain.title.blocked",
      blockedHighScore: "candidate.explain.title.blockedHighScore",
      ignore: "candidate.explain.title.ignore",
    }[kind] || "candidate.explain.title.recommend";
    return t(key);
  }

  function candidateExplainPrefix(kind) {
    const key = {
      recommend: "candidate.card.summary.prefix.recommend",
      watch: "candidate.card.summary.prefix.watch",
      blocked: "candidate.card.summary.prefix.blocked",
      blockedHighScore: "candidate.card.summary.prefix.blockedHighScore",
      ignore: "candidate.card.summary.prefix.ignore",
    }[String(kind || "").trim()] || "candidate.card.summary.prefix.recommend";
    return t(key);
  }

  function buildCandidateReviewTrail(candidate, side) {
    const status = candidateStatusValue(candidate);
    const [, statusLabel] = candidateStatusMeta(status);
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
      label: t("candidate.trail.discovery.label"),
      value: t("candidate.trail.discovery.value", {
        wallet: shortWallet(candidate.wallet),
        triggerType: candidate.trigger_type || t("candidate.trail.discovery.triggerTypeDefault"),
      }),
      detail: t("candidate.trail.discovery.detail", {
        walletCount: candidateWalletCountText(candidate.source_wallet_count || 1),
        walletTier: walletTierLabel(candidate.wallet_tier || t("candidate.shared.walletTierDefault")),
      }),
      tone: candidate.source_wallet_count > 1 ? "positive" : "warn",
    });
    trail.push({
      label: t("candidate.trail.enrichment.label"),
      value: t("candidate.trail.enrichment.value", {
        marketTag: candidate.market_tag || t("candidate.trail.enrichment.marketDefault"),
        bucket: candidate.resolution_bucket || t("candidate.trail.enrichment.bucketDefault"),
      }),
      detail: t("candidate.trail.enrichment.detail", {
        market: candidate.market_slug || candidate.token_id || "-",
        condition: candidate.condition_id || t("candidate.trail.enrichment.noCondition"),
      }),
      tone: candidate.market_tag ? "positive" : "neutral",
    });
    trail.push({
      label: t("candidate.trail.score.label"),
      value: t("candidate.trail.score.value", {
        score: Number(candidate.score || 0).toFixed(1),
        walletScore: Number(candidate.wallet_score || 0).toFixed(1),
        confidence: fmtPct(Number(candidate.confidence || 0) * 100, 0),
      }),
      detail: t("candidate.trail.score.detail", {
        sourcePrice: candidate.source_avg_price
          ? t("candidate.trail.score.sourcePrice", { price: Number(candidate.source_avg_price).toFixed(3) })
          : t("candidate.trail.score.noSourcePrice"),
        action: candidateActionText(action, side),
      }),
      tone: candidate.score >= 75 ? "positive" : candidate.score < 50 ? "danger" : "warn",
    });
    trail.push({
      label: t("candidate.trail.orderbook.label"),
      value: candidateOrderbookSummary(candidate),
      detail: t("candidate.trail.orderbook.detail", {
        spread: candidate.spread_pct == null ? "--" : fmtPct(candidate.spread_pct, 1),
        chase: candidate.chase_pct == null ? "--" : fmtPct(candidate.chase_pct, 1),
      }),
      tone: Number(candidate.chase_pct || 0) > 4 || Number(candidate.spread_pct || 0) > 6 ? "danger" : "neutral",
    });
    trail.push({
      label: t("candidate.trail.decision.label"),
      value: candidateActionText(candidate.suggested_action, side),
      detail: reasonText || candidate.recommendation_reason || t("candidate.trail.decision.waitingReason"),
      tone: candidateExplainTone(candidateExplainKind(candidate)),
    });
    trail.push({
      label: t("candidate.trail.review.label"),
      value: reviewAction ? candidateActionText(reviewAction, side) : t("candidate.trail.review.waiting"),
      detail: t("candidate.trail.review.detail", {
        status: reviewStatus || t("candidate.trail.review.waiting"),
        noteSuffix: reviewNote ? ` · ${reviewNote}` : "",
      }),
      tone: reviewStatus === "executed" ? "positive" : reviewStatus === "ignored" ? "cancel" : reviewStatus === "pending" ? "warn" : "neutral",
    });
    trail.push({
      label: t("candidate.trail.lifecycle.label"),
      value: expiresTs > 0
        ? t("candidate.trail.lifecycle.value", {
          status: statusLabel,
          age: fmtAge(Math.max(0, expiresTs - Math.floor(Date.now() / 1000))),
        })
        : statusLabel,
      detail: t("candidate.trail.lifecycle.detail", {
        created: createdTs > 0
          ? t("candidate.trail.lifecycle.createdAt", { time: fmtDateTime(createdTs) })
          : t("candidate.trail.lifecycle.createdUnknown"),
        updatedSuffix: updatedTs > 0
          ? ` · ${t("candidate.trail.lifecycle.updatedAt", { time: fmtDateTime(updatedTs) })}`
          : "",
        signalSuffix: signalTs > 0
          ? ` · ${t("candidate.trail.lifecycle.signalAt", { time: fmtDateTime(signalTs) })}`
          : "",
      }),
      tone: status === "executed" ? "positive" : status === "rejected" || status === "ignored" ? "danger" : "warn",
    });
    return trail;
  }

  function humanizeReason(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    return translateRegisteredLabel("enum.reason", raw, normalizeMetricName);
  }

  function issueDisplayText(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    return humanizeReason(raw) || humanizeIdentifier(raw);
  }

  function candidateGateReason(candidate) {
    const skipReason = humanizeReason(candidateField(candidate, "skip_reason") || candidate.skip_reason);
    const riskReason = humanizeReason(candidateField(candidate, "risk_reason") || candidate.risk_reason);
    return skipReason || riskReason || "";
  }

  function candidateGateState(candidate, side) {
    const reason = candidateGateReason(candidate);
    const score = Number(candidateField(candidate, "score") || candidate.score || 0);
    const highScore = score >= 70;
    if (!reason) {
      return {
        gated: false,
        label: t("candidate.gate.passed.label"),
        detail: t("candidate.gate.passed.detail"),
        tone: "positive",
        score,
      };
    }
    const suggested = String(candidateField(candidate, "suggested_action") || candidate.suggested_action || "").trim().toLowerCase();
    const action = candidateActionText(suggested || candidate.suggested_action, side);
    return {
      gated: true,
      label: highScore
        ? t("candidate.gate.blockedHighScore.label")
        : t("candidate.gate.blocked.label"),
      detail: highScore
        ? t("candidate.gate.blockedHighScore.detail", { score: score.toFixed(1), reason })
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
    return t("candidate.orderbook.summary", { bid, ask, mid });
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
        label: skipReason
          ? t("candidate.explain.labels.actionSuggested")
          : t("candidate.explain.labels.currentAction"),
        value: candidateActionText(suggestedAction, side),
      });
    }
    if (Number.isFinite(score) || Number.isFinite(walletScore) || Number.isFinite(confidence) || sourceWalletCount > 0) {
      lines.push({
        label: t("candidate.explain.labels.scoreBreakdown"),
        value: t("candidate.explain.values.scoreBreakdown", {
          score: score.toFixed(1),
          walletScore: walletScore.toFixed(1),
          confidence: fmtPct(confidence * 100, 0),
          walletCount: candidateWalletCountText(sourceWalletCount),
        }),
      });
    }
    if (skipReason || riskReason) {
      lines.push({
        label: t("candidate.explain.labels.skipOrRisk"),
        value: skipReason || riskReason,
      });
    }
    if (Number.isFinite(spreadPct) || Number.isFinite(chasePct)) {
      const parts = [];
      if (Number.isFinite(spreadPct) && spreadPct > 0) {
        parts.push(t("candidate.explain.values.spread", { spread: fmtPct(spreadPct, 1) }));
      }
      if (Number.isFinite(chasePct) && chasePct > 0) {
        parts.push(t("candidate.explain.values.chase", { chase: fmtPct(chasePct, 1) }));
      }
      if (parts.length > 0) {
        lines.push({
          label: t("candidate.explain.labels.momentumOrderbook"),
          value: parts.join(" · "),
        });
      }
    }
    if (sourceWalletCount > 1 || resonanceHint.includes("resonance") || walletScoreSummary.toLowerCase().includes("resonance")) {
      lines.push({
        label: t("candidate.explain.labels.resonanceSignal"),
        value: sourceWalletCount > 1
          ? t("candidate.explain.values.resonanceWallets", { count: sourceWalletCount })
          : (walletScoreSummary || t("candidate.explain.values.multiWalletResonance")),
      });
    }
    const conflictParts = [];
    if (existingPosition) conflictParts.push(t("candidate.explain.conflict.existingPosition"));
    if (duplicate) conflictParts.push(t("candidate.explain.conflict.duplicate"));
    if (nettingLimited) conflictParts.push(t("candidate.explain.conflict.nettingLimited"));
    if (budgetLimited) conflictParts.push(t("candidate.explain.conflict.budgetLimited"));
    if (cooldownRemaining > 0) {
      conflictParts.push(t("candidate.explain.conflict.cooldownRemaining", { age: fmtAge(cooldownRemaining) }));
    }
    if (conflictParts.length > 0) {
      lines.push({
        label: t("candidate.explain.labels.riskConflict"),
        value: conflictParts.join(" · "),
      });
    }
    if (walletScoreSummary) {
      lines.push({
        label: t("candidate.explain.labels.walletProfile"),
        value: walletScoreSummary,
      });
    }
    if (lines.length <= 0) {
      lines.push({
        label: t("candidate.explain.labels.generic"),
        value: t("candidate.explain.fallback"),
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
      return item && item.value && ![
        t("candidate.explain.labels.scoreBreakdown"),
        t("candidate.explain.labels.currentAction"),
        t("candidate.explain.labels.actionSuggested"),
      ].includes(label);
    });
    if (line && line.value) return String(line.value).trim();
    const sourceWalletCount = Math.max(1, Number(candidate.source_wallet_count || 1));
    return t("candidate.card.reasonFallback", {
      action: candidateActionText(candidate.suggested_action, side),
      walletCount: candidateWalletCountText(sourceWalletCount),
    });
  }

  function candidateCardPriorityItems(candidate, side) {
    const score = Number(candidate.score || 0);
    const walletScore = Number(candidate.wallet_score || 0);
    const observedNotional = Number(candidate.observed_notional || 0);
    const observedSize = Number(candidate.observed_size || 0);
    const qualityParts = [t("candidate.card.priority.scoreValue", { score: score.toFixed(0) })];
    if (walletScore > 0) {
      qualityParts.push(t("candidate.card.priority.walletValue", { walletScore: walletScore.toFixed(0) }));
    }
    const sizeValue = observedNotional > 0
      ? fmtUsd(observedNotional, false)
      : observedSize > 0
        ? t("candidate.card.priority.sizeShares", { shares: observedSize.toFixed(0) })
        : t("common.dash");
    return [
      {
        label: t("candidate.card.priority.score"),
        value: qualityParts.join(" · "),
      },
      {
        label: t("candidate.card.priority.size"),
        value: sizeValue,
      },
    ];
  }

  function candidateCardSummaryLead(candidate, side) {
    const explainKind = candidateExplainKind(candidate);
    const title = candidateExplainTitle(explainKind);
    const gateReason = candidateGateReason(candidate);
    const prefix = candidateExplainPrefix(explainKind);
    const action = candidateActionText(candidateActionValue(candidate) || candidate.suggested_action, side);
    const score = Number(candidate.score || 0);
    const walletScore = Number(candidate.wallet_score || 0);
    const confidence = Number(candidate.confidence || 0);
    const sourceWalletCount = Math.max(1, Number(candidate.source_wallet_count || 1));
    const reason = explainKind === "blockedHighScore" || explainKind === "blocked"
      ? gateReason || candidateCardReason(candidate, side)
      : candidateCardReason(candidate, side);
    const pieces = [];
    if (explainKind === "ignore") {
      const skip = gateReason;
      const spread = Number(candidate.spread_pct);
      const chase = Number(candidate.chase_pct);
      if (skip) pieces.push(skip);
      if (!skip && Number.isFinite(spread)) pieces.push(t("candidate.explain.values.spread", { spread: fmtPct(spread, 1) }));
      if (Number.isFinite(chase)) pieces.push(t("candidate.explain.values.chaseShort", { chase: fmtPct(chase, 1) }));
      if (score > 0) pieces.push(t("candidate.card.metric.score", { score: score.toFixed(0) }));
    } else if (explainKind === "blockedHighScore" || explainKind === "blocked") {
      pieces.push(t("candidate.card.metric.score", { score: score.toFixed(0) }));
      const gateReason = candidateGateReason(candidate);
      if (gateReason) pieces.push(gateReason);
      if (walletScore > 0) pieces.push(t("candidate.card.metric.wallet", { walletScore: walletScore.toFixed(0) }));
    } else if (explainKind === "watch") {
      pieces.push(action);
      if (sourceWalletCount > 1) pieces.push(candidateWalletCountText(sourceWalletCount));
      if (walletScore > 0) pieces.push(t("candidate.card.metric.wallet", { walletScore: walletScore.toFixed(0) }));
      if (confidence > 0) pieces.push(t("candidate.card.metric.confidence", { confidence: fmtPct(confidence * 100, 0) }));
    } else {
      pieces.push(action);
      if (score > 0) pieces.push(t("candidate.card.metric.score", { score: score.toFixed(0) }));
      if (sourceWalletCount > 1) pieces.push(candidateWalletCountText(sourceWalletCount));
      if (walletScore > 0) pieces.push(t("candidate.card.metric.wallet", { walletScore: walletScore.toFixed(0) }));
    }
    const tail = pieces.filter(Boolean).join(" · ");
    const combined = t("candidate.card.summary.full", {
      prefix,
      reason,
      tail: tail ? ` · ${tail}` : "",
    }, `${prefix}: ${reason}${tail ? ` · ${tail}` : ""}`);
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
      `${candidate.market_slug || "-"} · ${candidate.outcome || "--"} · ${sideLabel(side)}`,
      `${wallet}${candidate.wallet_tier ? ` · ${walletTierLabel(candidate.wallet_tier)}` : ""} · ${candidateWalletCountText(sourceWalletCount)}`,
      reason,
      gateState.gated ? `${gateState.label} · ${gateState.reason}` : t("candidate.card.hover.gatePassed"),
      t("candidate.card.hover.scoreLine", {
        score: Number(candidate.score || 0).toFixed(1),
        walletScore: Number(candidate.wallet_score || 0).toFixed(1),
        confidence: fmtPct(Number(candidate.confidence || 0) * 100, 0),
      }, `score ${Number(candidate.score || 0).toFixed(1)} · wallet ${Number(candidate.wallet_score || 0).toFixed(1)} · conf ${fmtPct(Number(candidate.confidence || 0) * 100, 0)}`),
      t("candidate.card.hover.orderbookLine", { bid, ask, mid, spread, chase }),
      `${action} · ${candidateCardReason(candidate, side)}`,
    ];
    for (const item of Array.isArray(priorityItems) ? priorityItems : []) {
      if (!item || !item.label || !item.value) continue;
      lines.push(t("candidate.card.hover.labelValue", { label: item.label, value: item.value }));
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
    if (value === "positive") return t("candidate.factor.pill.positive");
    if (value === "danger") return t("candidate.factor.pill.negative");
    if (value === "warn") return t("candidate.factor.pill.warn");
    return t("candidate.factor.pill.neutral");
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
    const walletTierText = walletTierLabel(walletTier || t("candidate.shared.walletTierDefault"));
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
    const explainKind = candidateExplainKind(candidate);
    const recommendedTone = candidateExplainTone(explainKind);
    const gateState = candidateGateState(candidate, side);
    const reasonFactors = candidateReasonFactors(candidate);

    const fallbackBreakdown = [
      {
        key: "gate",
        label: t("candidate.factor.breakdown.gate.label"),
        value: gateState.gated ? gateState.label : t("candidate.gate.passed.label"),
        detail: gateState.gated ? gateState.detail : t("candidate.gate.passed.detail"),
        tone: gateState.tone,
      },
      {
        key: "score",
        label: t("candidate.factor.breakdown.score.label"),
        value: score.toFixed(1),
        detail: recommendationReason || candidateActionText(candidate.suggested_action, side),
        tone: candidateFactorTone(score / 100, 0.9, 0.7),
      },
      {
        key: "wallet_score",
        label: t("candidate.factor.breakdown.walletScore.label"),
        value: `${walletScore.toFixed(1)}${walletTierText ? ` · ${walletTierText}` : ""}`,
        detail: String(candidate.wallet_tag || candidate.wallet_score_summary || t("candidate.factor.breakdown.walletScore.detailFallback")),
        tone: candidateFactorTone(walletScore / 100, 0.85, 0.6),
      },
      {
        key: "confidence",
        label: t("candidate.factor.breakdown.confidence.label"),
        value: fmtPct(confidence * 100, 0),
        detail: String(candidate.wallet_score_summary || recommendationReason || t("candidate.factor.breakdown.confidence.detailFallback")),
        tone: candidateFactorTone(confidence, 0.7, 0.45),
      },
      {
        key: "resonance_wallets",
        label: t("candidate.factor.breakdown.resonanceWallets.label"),
        value: candidateWalletCountText(sourceWalletCount),
        detail: sourceWalletCount > 1
          ? t("candidate.factor.breakdown.resonanceWallets.multi")
          : t("candidate.factor.breakdown.resonanceWallets.single"),
        tone: sourceWalletCount > 1 ? "positive" : "warn",
      },
      {
        key: "momentum",
        label: t("candidate.factor.breakdown.momentum.label"),
        value: momentumSummary,
        detail: momentum30m < 0
          ? t("candidate.factor.breakdown.momentum.pullback")
          : t("candidate.factor.breakdown.momentum.tailwind"),
        tone: momentum30m > 0 ? "positive" : momentum30m < -10 ? "danger" : "warn",
      },
      {
        key: "orderbook",
        label: t("candidate.factor.breakdown.orderbook.label"),
        value: `${spreadPct == null || spreadPct === "" ? "--" : fmtPct(spreadValue, 1)} / ${chasePct == null || chasePct === "" ? "--" : fmtPct(chaseValue, 1)}`,
        detail: t("candidate.factor.breakdown.orderbook.detail", {
          bid: currentBid == null ? "--" : currentBid.toFixed(3),
          ask: currentAsk == null ? "--" : currentAsk.toFixed(3),
          mid: currentMid == null ? "--" : currentMid.toFixed(3),
        }),
        tone: costPressure == null ? "warn" : costPressure > 6 ? "danger" : costPressure > 2 ? "warn" : "positive",
      },
      {
        key: "trigger_type",
        label: t("candidate.factor.breakdown.triggerType.label"),
        value: triggerType || t("candidate.trail.discovery.triggerTypeDefault"),
        detail: String(candidate.market_tag || candidate.resolution_bucket || t("candidate.factor.breakdown.triggerType.detailFallback")),
        tone: recommendedTone,
      },
    ];

    const fallbackConflicts = [];
    if (existingPosition) {
      fallbackConflicts.push({
        key: "existing_position",
        label: t("candidate.factor.conflict.existingPosition.label"),
        value: existingNotional > 0 ? fmtUsd(existingNotional, false) : t("candidate.factor.conflict.existingPosition.valueFallback"),
        detail: t("candidate.factor.conflict.existingPosition.detail"),
        tone: "danger",
      });
    }
    if (duplicate) {
      fallbackConflicts.push({
        key: "duplicate",
        label: t("candidate.factor.conflict.duplicate.label"),
        value: t("candidate.factor.conflict.duplicate.value"),
        detail: t("candidate.factor.conflict.duplicate.detail"),
        tone: "warn",
      });
    }
    if (nettingLimited) {
      fallbackConflicts.push({
        key: "netting_limited",
        label: t("candidate.factor.conflict.netting.label"),
        value: t("candidate.factor.conflict.netting.value"),
        detail: t("candidate.factor.conflict.netting.detail"),
        tone: "warn",
      });
    }
    if (budgetLimited) {
      fallbackConflicts.push({
        key: "budget_limited",
        label: t("candidate.factor.conflict.budget.label"),
        value: t("candidate.factor.conflict.budget.value"),
        detail: t("candidate.factor.conflict.budget.detail"),
        tone: "danger",
      });
    }
    if (cooldownRemaining > 0) {
      fallbackConflicts.push({
        key: "cooldown",
        label: t("candidate.factor.conflict.cooldown.label"),
        value: fmtAge(cooldownRemaining),
        detail: t("candidate.factor.conflict.cooldown.detail"),
        tone: "warn",
      });
    }
    if (riskReason) {
      fallbackConflicts.push({
        key: "risk_reason",
        label: t("candidate.factor.conflict.riskReason.label"),
        value: riskReason,
        detail: skipReason || t("candidate.factor.conflict.riskReason.detailFallback"),
        tone: "danger",
      });
    } else if (skipReason) {
      fallbackConflicts.push({
        key: "skip_reason",
        label: t("candidate.factor.conflict.skipReason.label"),
        value: skipReason,
        detail: t("candidate.factor.conflict.skipReason.detail"),
        tone: "warn",
      });
    }
    if (fallbackConflicts.length <= 0) {
      fallbackConflicts.push({
        key: "none",
        label: t("candidate.factor.conflict.none.label"),
        value: t("candidate.factor.conflict.none.value"),
        detail: t("candidate.factor.conflict.none.detail"),
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
              <span>${escapeHtml(String(item && item.label || t("candidate.factor.card.defaultLabel")))}</span>
              <span class="factor-pill factor-${pillTone}">${pillText}</span>
            </div>
            <b class="${valueClass}">${escapeHtml(String(item && item.value || "--"))}</b>
            ${weightText ? `<small class="factor-weight">${escapeHtml(t("candidate.factor.card.weight", { weight: weightText }))}</small>` : ""}
            <p class="factor-detail">${escapeHtml(String(item && item.detail || ""))}</p>
          </div>`;
        }).join("")
      : `<div class="component-card factor-card factor-neutral"><span>${t("candidate.factor.card.defaultLabel")}</span><b>${t("common.waitingData")}</b><p>${t("candidate.factor.card.empty")}</p></div>`;
  }

  function renderCandidateSummaryCards(items) {
    const rows = Array.isArray(items) ? items.filter((item) => item && typeof item === "object") : [];
    if (rows.length <= 0) {
      return `<div class="component-card candidate-summary-card factor-card factor-neutral"><span>${t("candidate.summary.card.title")}</span><b>${t("common.waitingData")}</b><p>${t("candidate.summary.card.empty")}</p></div>`;
    }

    const buildCard = (item, featured = false) => {
      const tone = normalizeFactorTone(item && item.tone || "neutral");
      const valueClass = candidateFactorValueClass(tone);
      const rawWeight = Number(item && item.weight);
      const weightText = Number.isFinite(rawWeight) && Math.abs(rawWeight) > 0.05 ? `${rawWeight >= 0 ? "+" : ""}${rawWeight.toFixed(1)}` : "";
      const pillTone = factorPillTone(item && item.rawDirection ? item.rawDirection : tone);
      const pillText = factorPillText(item && item.rawDirection ? item.rawDirection : tone);
      const label = escapeHtml(String(item && item.label || t("candidate.factor.card.defaultLabel")));
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
          ${weightText ? `<small class="factor-weight">${escapeHtml(t("candidate.factor.card.weight", { weight: weightText }))}</small>` : ""}
          <p class="candidate-summary-detail">${detail}</p>
        </article>`;
      }
      return `<article class="component-card candidate-summary-card factor-card factor-${tone || "neutral"}">
        <div class="candidate-summary-card-head">
          <span>${label}</span>
          <span class="factor-pill factor-${pillTone}">${pillText}</span>
        </div>
        <b class="${valueClass}">${value}</b>
        ${weightText ? `<small class="factor-weight">${escapeHtml(t("candidate.factor.card.weight", { weight: weightText }))}</small>` : ""}
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
    const scoreCard = factorCards.breakdown.find((item) => String(item.key || "").trim().toLowerCase() === "score") || factorCards.breakdown[0] || {};
    const momentumCard = factorCards.breakdown.find((item) => String(item.key || "").trim().toLowerCase() === "momentum") || factorCards.breakdown[4] || {};
    const costCard = factorCards.breakdown.find((item) => String(item.key || "").trim().toLowerCase() === "orderbook") || factorCards.breakdown[5] || {};
    const riskCard = factorCards.conflicts[0] || {};
    const actionText = candidateActionText(candidate.suggested_action, side);
    const gateState = candidateGateState(candidate, side);
    const strip = [
      {
        label: t("candidate.factor.breakdown.gate.label"),
        value: gateState.gated ? gateState.label : t("candidate.gate.passed.label"),
        detail: gateState.gated ? gateState.detail : t("candidate.gate.passed.detail"),
        tone: gateState.tone,
      },
      {
        label: t("candidate.focus.strip.decision.label"),
        value: actionText,
        detail: String(decisionFactor && decisionFactor.detail || candidate.recommendation_reason || candidate.skip_reason || candidate.note || candidate.wallet_score_summary || t("candidate.focus.strip.decision.detailFallback")),
        tone: normalizeFactorTone(decisionFactor && decisionFactor.tone || candidateExplainTone(candidateExplainKind(candidate))),
        rawDirection: decisionFactor && decisionFactor.rawDirection ? decisionFactor.rawDirection : "",
        weight: decisionFactor && Number.isFinite(Number(decisionFactor.weight)) ? Number(decisionFactor.weight) : null,
      },
      {
        label: t("candidate.factor.breakdown.score.label"),
        value: String(scoreCard.value || "0.0"),
        detail: String(scoreCard.detail || t("candidate.focus.strip.score.detailFallback")),
        tone: String(scoreCard.tone || "neutral"),
      },
      {
        label: t("candidate.factor.breakdown.momentum.label"),
        value: String(momentumCard.value || "--"),
        detail: String(momentumCard.detail || t("candidate.focus.strip.momentum.detailFallback")),
        tone: String(momentumCard.tone || "neutral"),
      },
      {
        label: t("candidate.factor.breakdown.orderbook.label"),
        value: String(costCard.value || "--"),
        detail: String(costCard.detail || t("candidate.focus.strip.orderbook.detailFallback")),
        tone: String(costCard.tone || "neutral"),
      },
      {
        label: t("candidate.focus.strip.risk.label"),
        value: String(riskCard.value || t("candidate.factor.conflict.none.value")),
        detail: String(riskCard.detail || t("candidate.focus.strip.risk.detailFallback")),
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
    return buildCandidateReviewTrail(candidate, side);
  }

  function candidateObjectRows(payload, limit = 5) {
    if (!payload || typeof payload !== "object") return [];
    return Object.entries(payload)
      .filter(([, value]) => value != null && String(value).trim() !== "")
      .slice(0, limit)
      .map(([key, value]) => ({
        key: translateMetricLabel(key) || humanizeIdentifier(key),
        value: Array.isArray(value)
          ? value.join(" / ")
          : typeof value === "object"
            ? t("common.notRecorded")
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
      overviewEl.innerHTML = `<div class="component-card"><span>${escapeHtml(t("candidate.detail.page"))}</span><b>${escapeHtml(t("common.waitingData"))}</b></div>`;
      timelineEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.timeline"))}</span><b>${escapeHtml(t("common.waitingData"))}</b></div></li>`;
      chainEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.decisionChain"))}</span><b>${escapeHtml(t("common.waitingData"))}</b></div></li>`;
      actionsEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.action"))}</span><b>${escapeHtml(t("common.waitingData"))}</b></div></li>`;
      journalEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.journal"))}</span><b>${escapeHtml(t("common.waitingData"))}</b></div></li>`;
    };

    if (!candidate || typeof candidate !== "object") {
      metaEl.textContent = t("candidate.detail.runtime.empty.meta");
      emptyLists();
      return;
    }

    const candidateId = candidateKey(candidate);
    const detailReady = candidateDetailMatches(candidate, lastCandidateDetail);
    const pending = lastCandidateDetailApiState.pending && String(lastCandidateDetailApiState.candidateId || "") === candidateId;
    if (!detailReady && pending) {
      metaEl.textContent = t("candidate.detail.runtime.loading.metaValue", { candidateId: candidateId || t("common.entity.candidate") });
      overviewEl.innerHTML = `<div class="component-card"><span>${escapeHtml(t("candidate.detail.page"))}</span><b>${escapeHtml(t("common.loading"))}</b><p>${escapeHtml(t("candidate.detail.runtime.loading.body"))}</p></div>`;
      timelineEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.timeline"))}</span><b>${escapeHtml(t("common.loading"))}</b></div></li>`;
      chainEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.decisionChain"))}</span><b>${escapeHtml(t("common.loading"))}</b></div></li>`;
      actionsEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.action"))}</span><b>${escapeHtml(t("common.loading"))}</b></div></li>`;
      journalEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.journal"))}</span><b>${escapeHtml(t("common.loading"))}</b></div></li>`;
      return;
    }

    if (!detailReady && !lastCandidateDetailApiState.ok && String(lastCandidateDetailApiState.candidateId || "") === candidateId) {
      metaEl.textContent = t("candidate.detail.runtime.unavailable.metaValue", { candidateId: candidateId || t("common.entity.candidate") });
      overviewEl.innerHTML = `<div class="component-card"><span>${escapeHtml(t("candidate.detail.page"))}</span><b>${escapeHtml(t("candidate.detail.runtime.unavailable.title"))}</b><p>${escapeHtml(lastCandidateDetailApiState.error || t("candidate.detail.runtime.unavailable.body"))}</p></div>`;
      timelineEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.timeline"))}</span><b>${escapeHtml(t("common.unavailable"))}</b></div></li>`;
      chainEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.decisionChain"))}</span><b>${escapeHtml(t("common.unavailable"))}</b></div></li>`;
      actionsEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.action"))}</span><b>${escapeHtml(t("common.unavailable"))}</b></div></li>`;
      journalEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.journal"))}</span><b>${escapeHtml(t("common.unavailable"))}</b></div></li>`;
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
    const traceStatusLabel = te("trace_status", traceStatus, traceStatus);

    metaEl.textContent = t("candidate.detail.runtime.metaValue", {
      label: candidate.market_slug || candidate.token_id || t("common.entity.candidate"),
      timelineCount: timeline.length,
      chainCount: decisionChain.length,
    });
    overviewEl.innerHTML = [
      `<div class="component-card candidate-gate-card candidate-gate-${gateState.tone}"><span>${t("candidate.detail.runtime.overview.gateTitle")}</span><b>${escapeHtml(gateState.label)}</b><p>${escapeHtml(gateState.reason || gateState.detail || t("candidate.detail.runtime.overview.gateDetail"))}${gateState.gated && Number(candidate.score || 0) > 0 ? ` · ${t("candidate.detail.runtime.overview.scoreSuffix", { score: Number(candidate.score || 0).toFixed(1) })}` : ""}</p></div>`,
      `<div class="component-card"><span>${t("candidate.detail.runtime.overview.traceTitle")}</span><b>${escapeHtml(String(trace.trace_id || candidate.trace_id || "--"))}</b><p>${traceOpenedTs > 0 ? t("candidate.detail.runtime.overview.traceOpened", { time: fmtDateTime(traceOpenedTs) }) : t("candidate.detail.runtime.overview.traceOpenedMissing")}</p></div>`,
      `<div class="component-card"><span>${t("candidate.detail.runtime.overview.statusTitle")}</span><b class="${traceStatus === "open" || traceStatus === "approved" ? "value-positive" : traceStatus === "closed" || traceStatus === "executed" ? "value-neutral" : "value-negative"}">${escapeHtml(traceStatusLabel)}</b><p>${t("candidate.detail.runtime.overview.statusDetail", {
        orders: Number(summary.order_count || orders.length || 0),
        actions: Number(summary.related_action_count || relatedActions.length || 0),
      })}</p></div>`,
      `<div class="component-card"><span>${t("candidate.detail.runtime.overview.chainTitle")}</span><b>${t("candidate.detail.runtime.overview.chainValue", { count: Number(summary.decision_chain_count || decisionChain.length || 0) })}</b><p>${t("candidate.detail.runtime.overview.chainDetail", {
        cycles: Number(summary.cycle_count || 0),
        trace: summary.trace_found ? t("candidate.detail.runtime.overview.traceFound") : t("candidate.detail.runtime.overview.traceMissing"),
      })}</p></div>`,
      `<div class="component-card"><span>${t("candidate.detail.runtime.overview.latestTitle")}</span><b>${latestTs > 0 ? fmtDateTime(latestTs) : t("common.dash")}</b><p>${t("candidate.detail.runtime.overview.latestDetail", {
        journalCount: Number(summary.related_journal_count || relatedJournal.length || 0),
        action: candidateActionText(candidate.suggested_action, side),
      })}</p></div>`,
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
              <b>${Number(item.ts || 0) > 0 ? fmtDateTime(item.ts) : t("common.dash")}</b>
            </div>
            <div class="review-sub">${escapeHtml(detailText || t("candidate.detail.runtime.timeline.noDetail"))}</div>
          </li>`;
        }).join("")
      : `<li><div class="review-main"><span>${t("candidate.detail.timeline")}</span><b>${t("candidate.detail.runtime.timeline.empty")}</b></div></li>`;

    chainEl.innerHTML = decisionChain.length > 0
      ? decisionChain.map((item) => {
          const mainText = [
            item.action_label || item.action || "-",
            item.side ? sideLabel(item.side) : "",
            item.wallet_tier ? walletTierLabel(item.wallet_tier) : "",
          ].filter(Boolean).join(" · ");
          const detailText = [
            item.topic_label ? t("common.kvInline", { label: t("common.entity.topic"), value: item.topic_label }) : "",
            item.order_status ? t("common.kvInline", { label: t("common.entity.order"), value: item.order_status }) : "",
            item.order_notional ? fmtUsd(item.order_notional, false) : "",
            item.final_status || "",
          ].filter(Boolean).join(" · ");
          return `<li>
            <div class="review-main">
              <span>${escapeHtml(mainText)}</span>
              <b>${escapeHtml(String(item.cycle_id || "--"))}</b>
            </div>
            <div class="review-sub">${escapeHtml(detailText || t("candidate.detail.runtime.chain.noDetail"))}${Number(item.ts || 0) > 0 ? ` · ${fmtDateTime(item.ts)}` : ""}</div>
          </li>`;
        }).join("")
      : `<li><div class="review-main"><span>${t("candidate.detail.decisionChain")}</span><b>${t("candidate.detail.runtime.chain.empty")}</b></div></li>`;

    actionsEl.innerHTML = relatedActions.length > 0
      ? relatedActions.map((item) => {
          const notional = Number(item.notional || 0);
          return `<li>
            <div class="review-main">
              <span>${escapeHtml(candidateActionText(item.action, side))}</span>
              <b>${Number(item.created_ts || 0) > 0 ? fmtDateTime(item.created_ts) : t("common.dash")}</b>
            </div>
            <div class="review-sub">${escapeHtml(String(item.note || t("candidate.detail.runtime.actions.noNote")))}${notional > 0 ? ` · ${fmtUsd(notional, false)}` : ""}${item.status ? ` · ${escapeHtml(String(item.status))}` : ""}</div>
          </li>`;
        }).join("")
      : `<li><div class="review-main"><span>${t("candidate.detail.action")}</span><b>${t("candidate.detail.runtime.actions.empty")}</b></div></li>`;

    journalEl.innerHTML = relatedJournal.length > 0
      ? relatedJournal.map((item) => {
          const pnl = Number(item.pnl_realized || 0);
          return `<li>
            <div class="review-main">
              <span>${escapeHtml(candidateActionText(item.action, side))}</span>
              <b>${Number(item.created_ts || 0) > 0 ? fmtDateTime(item.created_ts) : t("common.dash")}</b>
            </div>
            <div class="review-sub">${escapeHtml(String(item.rationale || item.text || t("candidate.detail.runtime.journal.noNote")))}${item.result_tag ? ` · ${escapeHtml(String(item.result_tag))}` : ""}${pnl !== 0 ? ` · ${fmtUsd(pnl)}` : ""}</div>
          </li>`;
        }).join("")
      : `<li><div class="review-main"><span>${t("candidate.detail.journal")}</span><b>${t("candidate.detail.runtime.journal.empty")}</b></div></li>`;
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
      metaEl.textContent = t("candidate.focus.unavailable.meta");
      headEl.innerHTML = `<span class="tag danger">${escapeHtml(t("common.status.error"))}</span><span class="mono">${escapeHtml(t("candidate.focus.unavailable.title"))}</span>`;
      summaryEl.textContent = String(apiState.error || t("candidate.focus.unavailable.status"));
      listEl.innerHTML = `<li><span>${escapeHtml(t("candidate.detail.status"))}</span><b>${escapeHtml(t("candidate.focus.unavailable.status"))}</b></li>`;
      metricEl.innerHTML = `<div class="component-card"><span>${escapeHtml(t("candidate.detail.deepRead"))}</span><b>${escapeHtml(t("candidate.focus.unavailable.status"))}</b></div>`;
      breakdownMetaEl.textContent = t("candidate.detail.breakdownMeta");
      breakdownEl.innerHTML = `<div class="component-card factor-card factor-danger"><span>${escapeHtml(t("candidate.detail.breakdownCard"))}</span><b>${escapeHtml(t("candidate.focus.unavailable.status"))}</b><p>${escapeHtml(t("candidate.focus.unavailable.title"))}</p></div>`;
      conflictMetaEl.textContent = t("candidate.detail.conflictMeta");
      conflictEl.innerHTML = `<div class="component-card factor-card factor-danger"><span>${escapeHtml(t("candidate.detail.conflictCard"))}</span><b>${escapeHtml(t("candidate.focus.unavailable.status"))}</b><p>${escapeHtml(t("candidate.focus.unavailable.title"))}</p></div>`;
      factorsMetaEl.textContent = t("candidate.detail.factorsMeta");
      factorsEl.innerHTML = `<div class="component-card candidate-summary-card factor-card factor-danger"><span>${escapeHtml(t("candidate.detail.factors"))}</span><b>${escapeHtml(t("candidate.focus.unavailable.status"))}</b><p>${escapeHtml(t("candidate.focus.unavailable.title"))}</p></div>`;
      trailMetaEl.textContent = t("candidate.detail.trailMeta");
      trailEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.trail"))}</span><b>${escapeHtml(t("candidate.focus.unavailable.status"))}</b></div></li>`;
      explainMetaEl.textContent = t("candidate.detail.explainMeta");
      explainEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.explainLabel"))}</span><b>${escapeHtml(t("common.waitingData"))}</b></div></li>`;
      snapshotEl.innerHTML = `<div class="component-card"><span>${escapeHtml(t("candidate.detail.snapshot"))}</span><b>${escapeHtml(t("common.waitingData"))}</b></div>`;
      actionEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.records"))}</span><b>${escapeHtml(t("candidate.focus.unavailable.status"))}</b></div></li>`;
      statusEl.textContent = t("candidate.focus.unavailable.status");
      renderCandidateDetailPanel(null, "BUY");
      return;
    }

    if (items.length <= 0) {
      const observability = candidateObservability(payload);
      const recentCycles = observability && typeof observability.recent_cycles === "object" ? observability.recent_cycles : {};
      const recentSignals = recentReviewCandidates(lastSignalReview || {}, 5);
      const recentTopSkip = topRecentCycleSkipReason(observability);
      detailRoot.classList.remove("panel-error");
      metaEl.textContent = t("candidate.focus.replay.meta");
      headEl.innerHTML = `<span class="tag wait">${escapeHtml(t("candidate.focus.replay.badge"))}</span><span class="mono">${escapeHtml(t("candidate.focus.replay.title"))}</span>`;
      summaryEl.textContent = t("candidate.focus.replay.summary", {
        cycles: Number(recentCycles.cycles || 0),
        signals: Number(recentCycles.signals || 0),
      });
      listEl.innerHTML = [
        `<li><span>${escapeHtml(t("candidate.focus.replay.list.cycles"))}</span><b>${t("candidate.focus.replay.values.cycles", { count: Number(recentCycles.cycles || 0) })}</b></li>`,
        `<li><span>${escapeHtml(t("candidate.focus.replay.list.signals"))}</span><b>${t("candidate.focus.replay.values.signals", { count: Number(recentCycles.signals || 0) })}</b></li>`,
        `<li><span>${escapeHtml(t("candidate.focus.replay.list.precheckSkipped"))}</span><b>${t("candidate.focus.replay.values.precheckSkipped", { count: Number(recentCycles.precheck_skipped || 0) })}</b></li>`,
        `<li><span>${escapeHtml(t("candidate.focus.replay.list.timeSource"))}</span><b>${t("candidate.focus.replay.timeSourceBreakdown", {
          metadata: Number(recentCycles.market_time_source && recentCycles.market_time_source.metadata || 0),
          legacy: Number(recentCycles.market_time_source && recentCycles.market_time_source.slug_legacy || 0),
          unknown: Number(recentCycles.market_time_source && recentCycles.market_time_source.unknown || 0),
        })}</b></li>`,
        `<li><span>${escapeHtml(t("candidate.focus.replay.list.primaryReason"))}</span><b>${escapeHtml(recentTopSkip ? t("candidate.focus.replay.primaryReasonCount", {
          reason: humanizeReason(recentTopSkip[0]) || recentTopSkip[0],
          count: Number(recentTopSkip[1] || 0),
        }) : t("candidate.focus.replay.noPrimaryReason"))}</b></li>`,
      ].join("");
      metricEl.innerHTML = [
        `<div class="component-card"><span>${escapeHtml(t("candidate.detail.deepRead"))}</span><b>${escapeHtml(t("candidate.focus.replay.emptyQueue"))}</b></div>`,
        `<div class="component-card"><span>${escapeHtml(t("candidate.focus.replay.list.signals"))}</span><b>${t("candidate.focus.replay.values.replaySignals", { count: Number(recentSignals.length || 0) })}</b></div>`,
      ].join("");
      breakdownMetaEl.textContent = t("candidate.detail.breakdownMeta");
      breakdownEl.innerHTML = `<div class="component-card factor-card factor-neutral"><span>${escapeHtml(t("candidate.detail.breakdownCard"))}</span><b>${escapeHtml(t("candidate.focus.replay.emptyBreakdown"))}</b><p>${escapeHtml(t("candidate.focus.replay.title"))}</p></div>`;
      conflictMetaEl.textContent = t("candidate.detail.conflictMeta");
      conflictEl.innerHTML = `<div class="component-card factor-card factor-neutral"><span>${escapeHtml(t("candidate.detail.conflictCard"))}</span><b>${escapeHtml(t("candidate.focus.replay.emptyConflict"))}</b><p>${escapeHtml(t("candidate.focus.replay.title"))}</p></div>`;
      factorsMetaEl.textContent = t("candidate.detail.factorsMeta");
      factorsEl.innerHTML = `<div class="component-card candidate-summary-card factor-card factor-neutral"><span>${escapeHtml(t("candidate.detail.factors"))}</span><b>${escapeHtml(t("candidate.focus.replay.modeTitle"))}</b><p>${escapeHtml(t("candidate.focus.replay.title"))}</p></div>`;
      trailMetaEl.textContent = t("candidate.focus.replay.metaSignals", { count: recentSignals.length });
      trailEl.innerHTML = recentSignals.length > 0
        ? recentSignals.map((item) => `<li><div class="review-main"><span>${escapeHtml(item.title || item.market_slug || item.token_id || "-")}</span><b>${Number(item.cycle_ts || 0) > 0 ? fmtDateTime(item.cycle_ts) : "--"}</b></div><div class="review-sub">${escapeHtml([item.side ? sideLabel(item.side) : "", item.action_label || item.final_status || t("common.entity.signal"), item.skip_reason ? (humanizeReason(item.skip_reason) || item.skip_reason) : "", item.wallet_tier ? t("candidate.focus.replay.walletTier", { tier: walletTierLabel(item.wallet_tier) }) : ""].filter(Boolean).join(" · "))}</div></li>`).join("")
        : `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.trail"))}</span><b>${escapeHtml(t("candidate.focus.replay.noRecentSignals"))}</b></div></li>`;
      explainMetaEl.textContent = t("candidate.detail.explainMeta");
      explainEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.explainLabel"))}</span><b>${escapeHtml(t("candidate.focus.replay.title"))}</b></div></li>`;
      snapshotEl.innerHTML = `<div class="component-card"><span>${t("candidate.detail.snapshot")}</span><b>${t("candidate.focus.replay.snapshotValue", { signals: Number(recentCycles.signals || 0) })}</b></div>`;
      actionEl.innerHTML = `<li><div class="review-main"><span>${escapeHtml(t("candidate.detail.records"))}</span><b>${escapeHtml(t("candidate.focus.replay.auditOnly"))}</b></div></li>`;
      statusEl.textContent = recentSignals.length > 0 ? t("candidate.focus.replay.meta") : t("candidate.focus.replay.statusEmpty");
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
          label: String(line && line.label || t("candidate.detail.explainLabel")),
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

    metaEl.textContent = t("candidate.focus.runtime.metaValue", {
      label: candidate.market_slug || candidate.token_id || t("common.entity.candidate"),
      trigger: candidate.trigger_type || t("candidate.trail.discovery.triggerTypeDefault"),
      id: candidateKey(candidate),
    });
    headEl.innerHTML =
      `<span class="tag ${statusCls}">${statusText}</span>` +
      `<span class="tag ${side === "SELL" ? "danger" : "ok"}">${sideLabel(side)}</span>` +
      `<span class="tag ${gateState.gated ? gateState.tone : "ok"}">${gateState.gated ? gateState.label : t("candidate.focus.runtime.head.gatePassed")}</span>` +
      `<span class="tag ${candidate.suggested_action === "watch" ? "wait" : candidate.suggested_action === "ignore" ? "cancel" : "ok"}">${candidateActionText(candidate.suggested_action, side)}</span>` +
      `<span class="tag ${hasConflict ? "danger" : "ok"}">${hasConflict ? t("candidate.focus.runtime.head.hasConflict") : t("candidate.focus.runtime.head.noConflict")}</span>`;
    summaryEl.textContent = candidateCardSummaryLead(candidate, side);
    listEl.innerHTML = [
      `<li><span>${t("candidate.focus.runtime.list.oneLine")}</span><b>${escapeHtml(candidateCardSummaryLead(candidate, side))}</b></li>`,
      `<li><span>${t("candidate.focus.runtime.list.executionGate")}</span><b class="${gateState.gated ? "value-negative" : "value-positive"}">${escapeHtml(gateState.label)} · ${escapeHtml(gateState.reason || gateState.detail || t("candidate.focus.runtime.values.passed"))}</b></li>`,
      `<li><span>${t("candidate.focus.runtime.list.sourceWallet")}</span><b>${shortWallet(candidate.wallet)}${candidate.wallet_tag ? ` · ${candidate.wallet_tag}` : ""}${candidate.wallet_tier ? ` · ${walletTierLabel(candidate.wallet_tier)}` : ""}</b></li>`,
      `<li><span>${escapeHtml(t("candidate.focus.list.conditionToken"))}</span><b>${candidate.condition_id || "--"}${candidate.token_id ? ` · ${candidate.token_id}` : ""}</b></li>`,
      `<li><span>${escapeHtml(t("candidate.focus.list.observedNotional"))}</span><b>${t("candidate.focus.runtime.values.observedNotional", { notional: fmtUsd(candidate.observed_notional || 0, false), shares: Number(candidate.observed_size || 0).toFixed(2) })}</b></li>`,
      `<li><span>${escapeHtml(t("candidate.focus.list.sourcePrice"))}</span><b>${t("candidate.focus.runtime.values.sourcePrice", {
        price: Number(candidate.source_avg_price || 0).toFixed(4),
        bid: currentBid,
        ask: currentAsk,
        mid: currentMid,
      })}</b></li>`,
      `<li><span>${escapeHtml(t("candidate.focus.list.orderbookCost"))}</span><b>${t("candidate.focus.runtime.values.orderbookCost", { spread: spreadPct, chase: chasePct })}</b></li>`,
      `<li><span>${escapeHtml(t("candidate.focus.list.momentum"))}</span><b>${t("candidate.focus.runtime.values.momentum", { momentum5m, momentum30m })}</b></li>`,
      `<li><span>${escapeHtml(t("candidate.focus.list.existingPosition"))}</span><b>${hasConflict ? t("candidate.focus.runtime.values.existingPositionConflict", { notional: fmtUsd(existingNotional, false) }) : t("common.none")}</b></li>`,
      `<li><span>${escapeHtml(t("candidate.focus.list.suggestedAction"))}</span><b>${t("candidate.focus.runtime.values.suggestedAction", { action: candidateActionText(candidate.suggested_action, side), confidence: fmtPct(confidence * 100, 0) })}</b></li>`,
      `<li><span>${escapeHtml(t("candidate.focus.list.expiryFreshness"))}</span><b>${t("candidate.focus.runtime.values.expiryFreshness", {
        expiry: candidate.expires_ts > 0 ? `${fmtAge(Math.max(0, Number(candidate.expires_ts || 0) - Math.floor(Date.now() / 1000)))}` : t("candidate.focus.runtime.values.longLived"),
        freshness: t("common.ageLabel", { age: fmtAge(ageSec) }),
      })}</b></li>`,
    ].join("");

    metricEl.innerHTML = [
      `<div class="component-card"><span>${t("candidate.focus.runtime.metrics.score")}</span><b class="${clsForScore(score)}">${score.toFixed(1)}</b></div>`,
      `<div class="component-card"><span>${t("candidate.focus.runtime.metrics.walletScore")}</span><b class="${clsForScore(candidate.wallet_score || 0)}">${Number(candidate.wallet_score || 0).toFixed(1)}</b></div>`,
      `<div class="component-card"><span>${t("candidate.focus.runtime.metrics.walletTier")}</span><b>${walletTierLabel(candidate.wallet_tier || "WATCH")}</b></div>`,
      `<div class="component-card"><span>${t("candidate.focus.runtime.metrics.sourceWalletCount")}</span><b>${Number(candidate.source_wallet_count || 1)}</b></div>`,
    ].join("");
    breakdownMetaEl.textContent = t("candidate.focus.runtime.metaCount.factors", { count: factorCards.breakdown.length });
    breakdownEl.innerHTML = renderFactorCards(factorCards.breakdown);
    conflictMetaEl.textContent = t("candidate.focus.runtime.metaCount.conflicts", { count: factorCards.conflicts.length });
    conflictEl.innerHTML = renderFactorCards(factorCards.conflicts);
    factorsMetaEl.textContent = t("candidate.focus.runtime.metaCount.factors", { count: factorStrip.length });
    factorsEl.innerHTML = renderCandidateSummaryCards(factorStrip);
    trailMetaEl.textContent = t("candidate.focus.runtime.metaCount.steps", { count: trail.length });
    trailEl.innerHTML = trail.length > 0
      ? trail.map((item) => `<li>
          <div class="review-main">
            <span>${escapeHtml(item.label)}</span>
            <b class="${candidateFactorValueClass(item.tone)}">${escapeHtml(item.value)}</b>
          </div>
          <div class="review-sub">${escapeHtml(item.detail || "")}</div>
        </li>`).join("")
      : `<li><div class="review-main"><span>${t("candidate.detail.trail")}</span><b>${t("candidate.focus.runtime.emptyTrail")}</b></div></li>`;

    explainMetaEl.textContent = t("candidate.focus.runtime.metaCount.lines", { count: explain.length });
    explainEl.innerHTML = explain.length > 0
      ? explain.map((line) => `<li><div class="review-main"><span>${escapeHtml(line.label)}</span><b>${escapeHtml(line.value || "")}</b></div></li>`).join("")
      : `<li><div class="review-main"><span>${t("candidate.detail.explainLabel")}</span><b>${t("candidate.focus.runtime.emptyExplain")}</b></div></li>`;

    snapshotEl.innerHTML = [
      `<div class="component-card"><span>${t("candidate.focus.runtime.snapshot.recommendationReason")}</span><b>${escapeHtml(candidate.recommendation_reason || t("common.none"))}</b></div>`,
      `<div class="component-card"><span>${t("candidate.focus.runtime.snapshot.marketTag")}</span><b>${escapeHtml(candidate.market_tag || t("candidate.focus.runtime.values.unlabeled"))}</b></div>`,
      `<div class="component-card"><span>${t("candidate.focus.runtime.snapshot.resolutionBucket")}</span><b>${escapeHtml(candidate.resolution_bucket || t("candidate.focus.runtime.values.unlabeled"))}</b></div>`,
      `<div class="component-card"><span>${t("candidate.focus.runtime.snapshot.conflict")}</span><b class="${hasConflict ? "value-negative" : "value-positive"}">${hasConflict ? t("candidate.focus.runtime.values.existingPosition") : t("candidate.focus.runtime.values.clear")}</b></div>`,
    ].join("");

    const snapshotLines = [];
    if (signalSnapshotRows.length > 0) {
      snapshotLines.push(`<li><div class="review-main"><span>${t("candidate.focus.runtime.snapshot.signal")}</span><b>${t("candidate.focus.runtime.metaCount.keys", { count: signalSnapshotRows.length })}</b></div><div class="review-sub">${signalSnapshotRows.map((row) => `${escapeHtml(row.key)}=${escapeHtml(row.value)}`).join(" · ")}</div></li>`);
    }
    if (topicSnapshotRows.length > 0) {
      snapshotLines.push(`<li><div class="review-main"><span>${t("candidate.focus.runtime.snapshot.topic")}</span><b>${t("candidate.focus.runtime.metaCount.keys", { count: topicSnapshotRows.length })}</b></div><div class="review-sub">${topicSnapshotRows.map((row) => `${escapeHtml(row.key)}=${escapeHtml(row.value)}`).join(" · ")}</div></li>`);
    }
    if (candidate.note) {
      snapshotLines.push(`<li><div class="review-main"><span>${t("candidate.focus.runtime.snapshot.note")}</span><b>${escapeHtml(candidate.note)}</b></div><div class="review-sub">${t("candidate.focus.runtime.snapshot.selectedAction", { action: escapeHtml(candidate.selected_action || "none") })}</div></li>`);
    }
    actionEl.innerHTML = snapshotLines.length > 0
      ? snapshotLines.join("")
      : `<li><div class="review-main"><span>${t("candidate.detail.records")}</span><b>${t("candidate.focus.runtime.emptyRecords")}</b></div></li>`;
    statusEl.textContent = snapshotLines.length > 0
      ? t("candidate.focus.runtime.metaCount.items", { count: snapshotLines.length })
      : t("candidate.focus.runtime.emptyRecords");
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
    if (data.local_available) configuredParts.push(te("notification_channel", "local"));
    if (data.webhook_configured) configuredParts.push(`${te("notification_channel", "webhook")}${webhookChannel && webhookChannel.target_count ? `×${Number(webhookChannel.target_count || 0)}` : ""}`);
    if (data.telegram_configured) configuredParts.push(te("notification_channel", "telegram"));
    metaEl.textContent = t("notifier.panel.meta", { count: recent.length });
    statusEl.textContent = configuredParts.length > 0
      ? t("notifier.panel.statusConfigured", {
          channels: configuredParts.join(" + "),
          updatedAt: data.updated_ts > 0 ? fmtDateTime(data.updated_ts) : t("common.waiting"),
        })
      : t("notifier.panel.statusNone");
    summaryEl.innerHTML = [
      `<div class="component-card"><span>${t("notifier.summary.local")}</span><b>${data.local_available ? t("common.available") : t("common.unavailable")}</b></div>`,
      `<div class="component-card"><span>${t("notifier.summary.webhook")}</span><b>${data.webhook_configured ? `${t("common.configured")}${webhookChannel && webhookChannel.target_count ? ` · ${t("notifier.summary.targets", { count: Number(webhookChannel.target_count || 0) })}` : ""}` : t("common.notConfigured")}</b></div>`,
      `<div class="component-card"><span>${t("notifier.summary.telegram")}</span><b>${data.telegram_configured ? t("common.configured") : t("common.notConfigured")}</b></div>`,
      `<div class="component-card"><span>${t("notifier.summary.deliveryStats")}</span><b>${t("notifier.summary.deliveryOk", { ok: Number(deliveryStats.ok_events || 0), total: Number(deliveryStats.event_count || 0) })}</b></div>`,
      `<div class="component-card"><span>${t("notifier.summary.updatedAt")}</span><b>${data.updated_ts > 0 ? fmtDateTime(data.updated_ts) : t("common.unknownShort")}</b></div>`,
    ].join("");
    detailEl.innerHTML = recent.length > 0
      ? recent.map((item) => {
          const [cls, label] = item.ok ? ["ok", t("notifier.event.ok")] : ["danger", t("notifier.event.fail")];
          const backend = String(item.backend || item.channel || "local");
          const backendLabel = channelDisplayName(backend);
          const title = translateKnownPhrase(String(item.title || t("notifier.empty.title")));
          const body = translateStructuredText(String(item.body || item.detail || ""));
          const deliveryCount = Number(item.delivery_count || (Array.isArray(item.deliveries) ? item.deliveries.length : 0) || 0);
          return `<li>
            <div class="review-main">
              <span>${escapeHtml(title)}</span>
              <b><span class="tag ${cls}">${escapeHtml(label)}</span> <span class="tag wait">${escapeHtml(backendLabel)}</span></b>
            </div>
            <div class="review-sub">${escapeHtml(body)}${deliveryCount > 0 ? ` · ${escapeHtml(t("notifier.list.deliveries", { count: deliveryCount }))}` : ""}${item.status_code ? ` · ${item.status_code}` : ""}${item.ts ? ` · ${fmtDateTime(item.ts)}` : ""}</div>
          </li>`;
        }).join("")
      : `<li><div class="review-main"><span>${t("notifier.list.recentEvents")}</span><b>${t("notifier.list.recentNone")}</b></div></li>`;
    eventsEl.innerHTML = recent.length > 0
      ? recent.map((item) => `<div class="component-card">
          <span>${escapeHtml(channelDisplayName(String(item.channel || item.backend || "local")))}</span>
          <b>${escapeHtml(translateKnownPhrase(String(item.title || t("notifier.empty.title"))))}</b>
          <p>${escapeHtml(translateStructuredText(String(item.body || item.detail || t("notifier.phrase.noBody"))))}</p>
        </div>`).join("")
        : channels.length > 0
          ? channels.map((item) => `<div class="component-card">
            <span>${escapeHtml(channelDisplayName(String(item.name || "channel")))}</span>
            <b>${item.configured ? t("common.configured") : t("common.notConfigured")}</b>
            <p>${escapeHtml(item.name === "webhook"
              ? t("notifier.summary.targets", { count: Number(item.target_count || 0) })
              : item.name === "telegram"
                ? t("notifier.channel.telegramMeta", {
                  chat: translateKnownPhrase(String(item.chat_id || t("notifier.phrase.chatUnavailable"))),
                  mode: notifierParseModeLabel(item.parse_mode),
                })
                : channelDisplayName(String(item.backend || "local")))}</p>
          </div>`).join("")
        : `<div class="component-card"><span>${t("notifier.list.recentEvents")}</span><b>${t("notifier.list.recentNone")}</b></div>`;
  }

  function renderBlockbeatsPanel(payload, apiState, now) {
    const data = payload && typeof payload === "object" ? payload : EMPTY_BLOCKBEATS;
    const state = apiState && typeof apiState === "object" ? apiState : { ok: true, error: "" };
    lastBlockbeats = data;
    lastBlockbeatsApiState = state;
    const metaEl = $("blockbeats-meta");
    const statusEl = $("blockbeats-status");
    const summaryEl = $("blockbeats-summary");
    const predictionEl = $("blockbeats-prediction-list");
    const importantEl = $("blockbeats-important-list");
    if (!metaEl || !statusEl || !summaryEl || !predictionEl || !importantEl) return;

    const renderNow = Number(now || 0) > 0 ? Number(now || 0) : Math.floor(Date.now() / 1000);
    const predictionFeed = data.prediction && typeof data.prediction === "object" ? data.prediction : EMPTY_BLOCKBEATS.prediction;
    const importantFeed = data.important && typeof data.important === "object" ? data.important : EMPTY_BLOCKBEATS.important;
    const predictionItems = Array.isArray(predictionFeed.items) ? predictionFeed.items : [];
    const importantItems = Array.isArray(importantFeed.items) ? importantFeed.items : [];
    const errors = Array.isArray(data.errors) ? data.errors.filter((item) => String(item || "").trim()) : [];
    const overallStatus = String(data.status || "").trim().toLowerCase();
    let statusCls = "ok";
    if (!state.ok || overallStatus === "error") statusCls = "danger";
    else if (overallStatus === "degraded" || predictionFeed.status !== "ok" || importantFeed.status !== "ok") statusCls = "wait";
    else if (predictionItems.length <= 0 && importantItems.length <= 0) statusCls = "wait";
    statusEl.className = `api-status ${statusCls}`;

    metaEl.textContent = t("blockbeats.meta", {
      predictionCount: predictionItems.length,
      importantCount: importantItems.length,
    });
    statusEl.textContent = !state.ok
      ? String(state.error || t("blockbeats.status.apiUnavailable"))
      : t("blockbeats.status.line", {
          status: te("report_status", overallStatus || "unknown", overallStatus || "unknown"),
          updatedAt: data.updated_ts > 0 ? `${fmtDateTime(data.updated_ts)} · ${historyAgeLabel(data.updated_ts, renderNow)}` : t("common.waiting"),
          issues: errors.length > 0 ? t("blockbeats.status.issuesSuffix", { count: errors.length }) : "",
        });

    summaryEl.innerHTML = [
      `<div class="component-card"><span>${t("blockbeats.summary.overallStatus")}</span><b>${escapeHtml(te("report_status", overallStatus || "unknown", (overallStatus || "unknown").toUpperCase()))}</b></div>`,
      `<div class="component-card"><span>${t("common.updatedAt")}</span><b>${data.updated_ts > 0 ? escapeHtml(fmtDateTime(data.updated_ts)) : escapeHtml(t("common.unknownShort"))}</b></div>`,
      `<div class="component-card"><span>${t("blockbeats.summary.predictionSource")}</span><b>${escapeHtml(blockbeatsFeedSourceLabel(predictionFeed.source))}</b></div>`,
      `<div class="component-card"><span>${t("blockbeats.summary.importantSource")}</span><b>${escapeHtml(blockbeatsFeedSourceLabel(importantFeed.source))}</b></div>`,
      `<div class="component-card"><span>${t("blockbeats.summary.errors")}</span><b>${errors.length > 0 ? escapeHtml(errors[0]) : escapeHtml(t("common.noneShort"))}</b></div>`,
    ].join("");

    predictionEl.innerHTML = renderBlockbeatsFeedItems(predictionFeed, renderNow, t("blockbeats.feed.empty", { label: t("blockbeats.feed.prediction.title") }));
    importantEl.innerHTML = renderBlockbeatsFeedItems(importantFeed, renderNow, t("blockbeats.feed.empty", { label: t("blockbeats.feed.important.title") }));
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

    metaEl.textContent = t("archive.meta", {
      stateAge: Number(data && data.ts || 0) > 0 ? historyAgeLabel(data.ts, now) : t("common.notRecorded"),
      monitor30Age: freshnessAgeLabel(report30Freshness),
      monitor12Age: freshnessAgeLabel(report12Freshness),
      eodAge: eodFreshness.ageLabel,
    });

    summaryEl.innerHTML = [
      `<div class="review-chip"><span>${t("archive.summary.candidates")}</span><b>${candidateCount}</b></div>`,
      `<div class="review-chip"><span>${t("archive.summary.pending")}</span><b>${pendingCount}</b></div>`,
      `<div class="review-chip"><span>${t("archive.summary.journal")}</span><b>${journalCount}</b></div>`,
      `<div class="review-chip"><span>${t("archive.summary.notifications")}</span><b>${notifierCount}</b></div>`,
      `<div class="review-chip"><span>${t("archive.summary.positions")}</span><b>${openPositions}</b></div>`,
      `<div class="review-chip"><span>${t("archive.summary.slots")}</span><b>${fmtPct(slotUtil, 1)}</b></div>`,
    ].join("");

    actionsEl.innerHTML = [
      { label: t("archive.action.stateBundleTitle"), action: "state_bundle", detail: t("archive.action.stateBundleDetail") },
      { label: t("archive.action.candidatesCsvTitle"), action: "candidates_csv", detail: t("archive.action.candidatesCsvDetail") },
      { label: t("archive.action.journalJsonTitle"), action: "journal_json", detail: t("archive.action.journalJsonDetail") },
      { label: t("archive.action.monitorBundleTitle"), action: "monitor_bundle", detail: t("archive.action.monitorBundleDetail") },
    ].map((item) => `
      <button class="btn export-btn" type="button" data-export-action="${item.action}">
        <span>${item.label}</span>
        <small>${item.detail}</small>
      </button>
    `).join("");

    statsEl.innerHTML = [
      `<div class="component-card"><span>${t("archive.stats.monitor")}</span><b><span class="tag ${monitor30Tag[0]}">${monitor30Tag[1]}</span> <span class="tag ${monitor12Tag[0]}">${monitor12Tag[1]}</span></b><p>${monitorWindowDisplaySummary(monitor30, 30 * 60, now)} / ${monitorWindowDisplaySummary(monitor12, 12 * 60 * 60, now)}</p></div>`,
      `<div class="component-card"><span>${t("archive.stats.eod")}</span><b><span class="tag ${eodTag[0]}">${eodTag[1]}</span></b><p>${t("archive.stats.internalVsLedger")} ${fmtUsd(eodGap)} · ${t("archive.stats.fills", { count: Number(eodRecon.fill_count_today || 0) })}</p></div>`,
      `<div class="component-card"><span>${t("archive.stats.walletPool")}</span><b>${t("archive.stats.profiles", { count: Number(walletProfiles.summary && walletProfiles.summary.count || 0) })}</b><p>${t("archive.stats.walletPoolDetail", {
        enabled: t("archive.stats.enabled", { count: Number(walletProfiles.summary && walletProfiles.summary.enabled || 0) }),
        localNotifier: t("archive.stats.localNotifier", {
          status: notifier.local_available ? t("common.available") : t("common.unavailable"),
        }),
      })}</p></div>`,
      `<div class="component-card"><span>${t("archive.stats.recentNotifications")}</span><b>${t("archive.stats.events", { count: notifierCount })}</b><p>${t("archive.stats.recentNotificationsDetail", {
        webhook: t("archive.stats.channelStatus", {
          channel: t("common.channel.webhook"),
          status: notifier.webhook_configured ? t("common.configured") : t("common.notConfigured"),
        }),
        telegram: t("archive.stats.channelStatus", {
          channel: t("common.channel.telegram"),
          status: notifier.telegram_configured ? t("common.configured") : t("common.notConfigured"),
        }),
        latest: t("archive.stats.latest", { title: notifier.last && notifier.last.title ? translateKnownPhrase(String(notifier.last.title)) : t("archive.stats.none") }),
      })}</p></div>`,
    ].join("");

    tagsEl.innerHTML = topTags.length > 0
      ? topTags.map((tag) => `<span class="tag wait">${escapeHtml(String(tag.tag || tag.label || t("archive.tags.defaultLabel")))} · ${Number(tag.count || 0)}</span>`).join("")
      : `<span class="tag wait">${t("archive.journalTagsEmpty")}</span>`;

    snapshotsEl.innerHTML = [
      `<div class="component-card"><span>${t("archive.snapshot.eod.title")}</span><b>${eod.day_key || "--"}</b><p>${t("archive.snapshot.eod.meta", {
        time: Number(eod.generated_ts || 0) > 0 ? fmtDateTime(eod.generated_ts) : t("common.notGenerated"),
        ageSuffix: Number(eod.generated_ts || 0) > 0 ? ` · ${historyAgeLabel(eod.generated_ts, now)}` : "",
        status: reportStatusMeta(eod.status || eodRecon.status || "unknown")[1],
      })}</p></div>`,
      `<div class="component-card"><span>${t("archive.snapshot.monitor30.title")}</span><b>${reportStatusMeta(monitor30.sample_status || "unknown")[1]}</b><p>${monitorWindowDisplaySummary(monitor30, 30 * 60, now)}</p></div>`,
      `<div class="component-card"><span>${t("archive.snapshot.monitor12.title")}</span><b>${reportStatusMeta(monitor12.sample_status || "unknown")[1]}</b><p>${monitorWindowDisplaySummary(monitor12, 12 * 60 * 60, now)}</p></div>`,
      `<div class="component-card"><span>${t("archive.snapshot.journal.title")}</span><b>${recentNotes.length}</b><p>${recentNotes[0] ? String(recentNotes[0].text || recentNotes[0].rationale || "").slice(0, 80) : t("archive.snapshot.journal.empty")}</p></div>`,
    ].join("");

    recentEl.innerHTML = notifierRecent.length > 0
      ? notifierRecent.map((item) => {
          const [cls, tag] = item.ok ? ["ok", t("common.status.ok")] : ["danger", t("common.status.fail")];
          return `<li>
            <div class="review-main">
              <span>${escapeHtml(translateKnownPhrase(String(item.title || t("archive.recentNotifications.itemTitleFallback"))))}</span>
              <b><span class="tag ${cls}">${tag}</span> <span class="tag wait">${escapeHtml(channelDisplayName(item.backend || item.channel || "local"))}</span></b>
            </div>
            <div class="review-sub">${escapeHtml(translateStructuredText(String(item.body || item.detail || "")))}${item.status_code ? ` · ${item.status_code}` : ""}${item.ts ? ` · ${fmtDateTime(item.ts)}` : ""}</div>
          </li>`;
        }).join("")
      : `<li><div class="review-main"><span>${t("archive.recentNotifications.emptyTitle")}</span><b>${t("archive.recentNotifications.emptyBody")}</b></div></li>`;
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
      if (!apiState.candidates.ok) parts.push(`${t("decision.apiUnavailableCandidates")} ${apiState.candidates.error || t("common.unavailable")}`);
      if (!apiState.mode.ok) parts.push(`${t("decision.apiUnavailableMode")} ${apiState.mode.error || t("common.unavailable")}`);
      return {
        cls: "danger",
        text: t("decision.apiError", { parts: parts.join(t("common.separator")) }),
      };
    }
    const count = Number(payload && payload.summary && payload.summary.count || 0);
    return {
      cls: count > 0 ? "ok" : "wait",
      text: count > 0
        ? t("decision.hasOpportunities", { mode: modeLabel(decision && decision.mode) })
        : t("decision.noOpportunities"),
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
    const recentReview = lastSignalReview && typeof lastSignalReview === "object" ? lastSignalReview : {};
    const recentReviewItems = recentReviewCandidates(recentReview, 6);
    controlState.decision_mode = mode;
    renderStatusMessage("candidate-panel-status", decisionPanelStatus(payload, decision, lastDecisionApiState), t("candidate.status.loading"));

    if ($("candidate-meta")) {
      const updatedTs = Number(decision.updated_ts || payload.updated_ts || 0);
      $("candidate-meta").textContent = t("decision.meta", {
        mode: modeLabel(mode),
        total: allItems.length,
        visible: filteredItems.length,
        sort: candidateSortLabel(candidateQueueFilter.sort),
        updatedAt: updatedTs > 0 ? t("decision.updatedAtSuffix", { updatedAt: fmtDateTime(updatedTs) }) : "",
      });
    }
    if ($("candidate-grid-meta")) {
      $("candidate-grid-meta").textContent = t("decision.gridMeta", {
        visible: filteredItems.length,
        total: allItems.length,
        view: candidateFocusViewLabel(candidateFocusView),
      }, `${filteredItems.length}/${allItems.length} visible · ${candidateFocusViewLabel(candidateFocusView)}`);
    }
    if ($("candidate-summary")) {
      const visibleSummary = candidateSummaryCounts(filteredItems);
      const observability = candidateObservability(payload);
      const metadataSummary = observability && typeof observability.market_metadata === "object" ? observability.market_metadata : {};
      const timeSourceSummary = observability && typeof observability.market_time_source === "object" ? observability.market_time_source : {};
      const topSkip = topCandidateSkipReason(observability);
      const recentCycles = observability && typeof observability.recent_cycles === "object" ? observability.recent_cycles : {};
      const recentTopSkip = topRecentCycleSkipReason(observability);
      $("candidate-summary").innerHTML = [
        `<span class="chip active">${escapeHtml(t("candidate.panel.summary.pending", { count: Number(visibleSummary.pending || 0) }))}</span>`,
        `<span class="chip">${escapeHtml(t("candidate.panel.summary.approved", { count: Number(visibleSummary.approved || 0) }))}</span>`,
        `<span class="chip">${escapeHtml(t("candidate.panel.summary.watched", { count: Number(visibleSummary.watched || 0) }))}</span>`,
        `<span class="chip">${escapeHtml(t("candidate.panel.summary.executed", { count: Number(visibleSummary.executed || 0) }))}</span>`,
        `<span class="chip">${escapeHtml(t("candidate.panel.summary.visible", { visible: filteredItems.length, total: allItems.length }))}</span>`,
        `<span class="chip">${escapeHtml(t("candidate.panel.summary.metadata", {
          hits: Number(metadataSummary.hits || 0),
          total: Number(observability.candidate_count || allItems.length || 0),
        }))}</span>`,
        `<span class="chip">${escapeHtml(t("candidate.panel.summary.timeSource", {
          metadata: Number(timeSourceSummary.metadata || 0),
          legacy: Number(timeSourceSummary.slug_legacy || 0),
          unknown: Number(timeSourceSummary.unknown || 0),
        }))}</span>`,
        topSkip ? `<span class="chip">${escapeHtml(humanizeReason(topSkip[0]) || String(topSkip[0] || ""))} ${Number(topSkip[1] || 0)}</span>` : "",
        Number(recentCycles.signals || 0) > 0 ? `<span class="chip">${escapeHtml(t("candidate.panel.summary.recentSignals", {
          cycles: Number(recentCycles.cycles || 0),
          signals: Number(recentCycles.signals || 0),
        }))}</span>` : "",
        recentTopSkip ? `<span class="chip">${escapeHtml(t("candidate.panel.summary.recentTopSkip", {
          reason: humanizeReason(recentTopSkip[0]) || String(recentTopSkip[0] || ""),
          count: Number(recentTopSkip[1] || 0),
        }))}</span>` : "",
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
      grid.innerHTML = `<article class="candidate-card candidate-empty candidate-error"><div><h3>${escapeHtml(t("candidate.panel.apiUnavailableTitle"))}</h3><p>${escapeHtml(lastDecisionApiState.candidates.error || t("candidate.panel.apiUnavailableBody"))}</p></div></article>`;
      renderCandidateFocus(visiblePayload, lastDecisionApiState);
      return;
    }
    if (filteredItems.length <= 0) {
      const hasFilters = Boolean(String(candidateQueueFilter.search || "").trim()) || String(candidateQueueFilter.status || "all") !== "all" || String(candidateQueueFilter.action || "all") !== "all" || String(candidateQueueFilter.side || "all") !== "all";
      if (!hasFilters && recentReviewItems.length > 0) {
        grid.innerHTML = recentReviewItems.map((candidate) => {
          const statusMeta = String(candidate.final_status || "").trim().toLowerCase();
          const [statusCls, statusText] = candidateReplayStatusMeta(statusMeta);
          const reason = candidate.skip_reason
            ? t("candidate.panel.replay.reason.skip", {
              reason: humanizeReason(candidate.skip_reason) || candidate.skip_reason,
            })
            : candidate.action_label
              ? t("candidate.panel.replay.reason.action", { action: candidate.action_label })
              : t("candidate.panel.replay.reason.default");
          return `
            <article class="candidate-card candidate-card-compact gated" title="${escapeHtml(reason)}">
              <div class="candidate-card-top">
                <div class="candidate-card-top-main">
                  <h3>${escapeHtml(candidate.title || candidate.market_slug || candidate.token_id || "-")}</h3>
                  <p class="candidate-card-kicker candidate-card-reason">${escapeHtml(reason)}</p>
                  <p class="candidate-card-gate candidate-card-gate-wait">${escapeHtml(t("candidate.panel.replay.gate"))}</p>
                </div>
                <div class="candidate-card-badges">
                  <span class="tag ${candidate.side === "SELL" ? "danger" : "ok"}">${escapeHtml(sideLabel(candidate.side || "BUY"))}</span>
                  <span class="tag ${statusCls}">${escapeHtml(statusText)}</span>
                </div>
              </div>
              <div class="candidate-card-strip">
                <div class="candidate-strip-item"><span>${t("candidate.panel.replay.strip.walletScore")}</span><b>${escapeHtml(Number(candidate.wallet_score || 0).toFixed(0))}</b></div>
                <div class="candidate-strip-item"><span>${t("candidate.panel.replay.strip.walletTier")}</span><b>${escapeHtml(candidate.wallet_tier ? walletTierLabel(candidate.wallet_tier) : "--")}</b></div>
                <div class="candidate-strip-item"><span>${t("candidate.panel.replay.strip.source")}</span><b>${escapeHtml(candidate.market_metadata_hit ? t("candidate.panel.replay.source.metadata") : (candidate.market_time_source || t("common.unknown")))}</b></div>
                <div class="candidate-strip-item"><span>${t("candidate.panel.replay.strip.cycleTime")}</span><b>${escapeHtml(Number(candidate.cycle_ts || 0) > 0 ? fmtDateTime(candidate.cycle_ts) : "--")}</b></div>
              </div>
              <div class="candidate-card-actions"><span class="mono">${escapeHtml(candidate.decision_reason || t("candidate.panel.replay.reviewOnly"))}</span></div>
            </article>
          `;
        }).join("");
      } else {
        grid.innerHTML = `<article class="candidate-card candidate-empty"><div><h3>${escapeHtml(hasFilters ? t("candidate.panel.emptyTitle.noMatch") : t("candidate.panel.emptyTitle.noOpportunity"))}</h3><p>${escapeHtml(hasFilters ? t("candidate.panel.emptyBody.noMatch") : t("candidate.panel.emptyBody.noOpportunity"))}</p></div></article>`;
      }
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
        ? String(actionState.message || t("common.actionState.submitFailed"))
        : actionState.kind === "success"
          ? String(actionState.message || t("common.actionState.submitted"))
          : actionState.kind === "pending"
            ? String(actionState.message || t("common.actionState.submitting"))
            : "";
      const secondaryActions = Array.isArray(actionPlan.secondary) ? actionPlan.secondary : [];
      const secondaryButtons = hasAction ? secondaryActions.map((button) => `
              <button class="candidate-action ${button.cls || "subtle"}" type="button" data-action="${button.action}">${button.label}</button>`).join("") : "";
      const showAction = hasAction || secondaryActions.length > 0;
      const cardMeta = [];
      if (gateState.gated) {
        cardMeta.push(`<span class="tag ${gateState.tone}">${escapeHtml(t("candidate.card.gateChip", { reason: gateState.reason }))}</span>`);
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
              <span class="tag ${side === "SELL" ? "danger" : "ok"}">${sideLabel(side)}</span>
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
      : `<span class="mono">${statusText ? escapeHtml(t("candidate.card.actionStatus", { status: statusText })) : escapeHtml(t("candidate.card.noAction"))}</span>`}
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
      $("wallet-profiles-meta").textContent = t("walletProfiles.meta", { count: items.length, enabled });
    }
    renderStatusMessage(
      "wallet-profiles-status",
      !lastWalletProfilesApiState.ok
        ? { cls: "danger", text: t("walletProfiles.status.apiError", { reason: lastWalletProfilesApiState.error || t("walletProfiles.empty.apiUnavailable") }) }
        : { cls: "wait", text: t("walletProfiles.status.ready") },
      t("walletProfiles.status.loading")
    );
    replaceRows(
      $("wallet-profiles-body"),
      items.map((item) => {
        const wallet = normalizeWallet(item.wallet);
        const draft = walletProfileDraft(item);
        const dirty = walletProfileChanged(item, draft);
        const requestState = walletProfileRequestState[wallet] || {};
        const requestLabel = requestState.kind === "error"
          ? String(requestState.message || t("walletProfiles.rowState.error"))
          : requestState.kind === "success"
            ? String(requestState.message || t("walletProfiles.rowState.saved"))
            : requestState.kind === "pending"
              ? String(requestState.message || t("walletProfiles.rowState.saving"))
              : dirty ? t("walletProfiles.rowState.dirty") : t("walletProfiles.rowState.synced");
        const requestCls = requestState.kind === "error" ? "danger" : requestState.kind === "success" ? "ok" : requestState.kind === "pending" ? "wait" : dirty ? "wait" : "cancel";
        return `
          <tr data-wallet-profile="${attrToken(wallet)}">
            <td class="wrap"><div class="cell-stack"><span class="cell-main">${shortWallet(item.wallet)}</span><span class="cell-sub">${escapeHtml(item.category || t("walletProfiles.field.unclassified"))}</span></div></td>
            <td><label class="inline-toggle"><input type="checkbox" data-wallet-profile-field="enabled" data-wallet-profile-key="${attrToken(wallet)}" ${draft.enabled ? "checked" : ""} /><span>${draft.enabled ? t("common.enabled") : t("common.disabled")}</span></label></td>
            <td><input class="editor-input wallet-profile-input" type="text" data-wallet-profile-field="tag" data-wallet-profile-key="${attrToken(wallet)}" value="${escapeHtml(draft.tag)}" placeholder="${escapeHtml(t("walletProfiles.field.tagPlaceholder"))}" /></td>
            <td class="wrap"><div class="cell-stack"><span class="cell-main ${clsForScore(item.trust_score)}">${Number(item.trust_score || 0).toFixed(1)}</span><span class="cell-sub">${t("walletProfiles.field.followability", { score: Number(item.followability_score || 0).toFixed(1) })}</span></div></td>
            <td class="wrap"><input class="editor-input wallet-profile-input" type="text" data-wallet-profile-field="notes" data-wallet-profile-key="${attrToken(wallet)}" value="${escapeHtml(draft.notes)}" placeholder="${escapeHtml(t("walletProfiles.field.notesPlaceholder"))}" /></td>
            <td class="wrap">
              <div class="wallet-profile-actions">
                <button class="btn btn-mini" type="button" data-wallet-profile-save="${attrToken(wallet)}">${t("common.save")}</button>
                <span class="mono wallet-profile-row-status ${requestCls === "danger" ? "value-negative" : requestCls === "ok" ? "value-positive" : "value-neutral"}">${escapeHtml(requestLabel)}</span>
              </div>
            </td>
          </tr>
        `;
      }),
      !lastWalletProfilesApiState.ok
        ? `<tr><td colspan="6">${t("walletProfiles.empty.apiUnavailable")}</td></tr>`
        : `<tr><td colspan="6">${t("walletProfiles.empty.none")}</td></tr>`
    );
  }

  function renderJournalPanel(payload, apiState = { ok: true, error: "" }) {
    const data = payload && typeof payload === "object" ? payload : EMPTY_JOURNAL;
    lastJournalSummary = data;
    lastJournalApiState = apiState && typeof apiState === "object" ? apiState : { ok: true, error: "" };
    const items = Array.isArray(data.recent) ? data.recent.slice(0, 6) : [];
    if ($("journal-meta")) {
      $("journal-meta").textContent = t("journal.meta", { count: items.length });
    }
    renderStatusMessage(
      "journal-panel-status",
      !lastJournalApiState.ok
        ? { cls: "danger", text: t("journal.status.apiError", { reason: lastJournalApiState.error || t("journal.empty.apiUnavailableReason") }) }
        : { cls: "wait", text: t("journal.status.ready", { count: Number(data.total_entries || 0) }) },
      t("journal.status.loading")
    );
    renderStatusMessage("journal-note-status", journalComposerNotice, t("journal.note.help"));
    const grid = $("journal-grid");
    if (!grid) return;
    if (!lastJournalApiState.ok && items.length <= 0) {
      grid.innerHTML = `<div class="component-card journal-card journal-card-error"><span>${t("journal.empty.apiUnavailableTitle")}</span><b>${t("journal.empty.apiUnavailableBody")}</b><p>${escapeHtml(lastJournalApiState.error || t("journal.empty.apiUnavailableReason"))}</p></div>`;
      return;
    }
    if (items.length <= 0) {
      grid.innerHTML = `<div class="component-card journal-card"><span>${t("journal.empty.noneTitle")}</span><b>${t("journal.empty.noneBody")}</b><p>${t("journal.empty.noneHelp")}</p></div>`;
      return;
    }
    grid.innerHTML = items.map((item) => `
      <div class="component-card journal-card">
        <span>${fmtDateTime(item.created_ts || item.ts || 0)}</span>
        <b>${candidateActionText(item.action, "BUY")}</b>
        <small>${escapeHtml(item.market_slug || shortWallet(item.wallet) || "-")}</small>
        <p>${escapeHtml(String(item.rationale || item.text || t("journal.note.missing")))}</p>
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
    const reconciliationIssues = Array.isArray(reconciliation.issues)
      ? reconciliation.issues.filter((item) => String(item || "").trim()).map((item) => issueDisplayText(item))
      : [];
    const eodIssues = Array.isArray(eod.issue_labels) && eod.issue_labels.length > 0
      ? eod.issue_labels
      : Array.isArray(eod.issues)
        ? eod.issues.map((item) => issueDisplayText(item))
        : [];
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
    const monitor30Label = t("opsGate.runtime.checks.monitor30Label");
    const monitor12Label = t("opsGate.runtime.checks.monitor12Label");
    const monitorFocus = [
      report30RawKind !== "ready" || report30Freshness.stale ? `${monitor30Label} ${monitorWindowDisplaySummary(report30, 30 * 60, nowTs)}` : "",
      report12RawKind !== "ready" || report12Freshness.stale ? `${monitor12Label} ${monitorWindowDisplaySummary(report12, 12 * 60 * 60, nowTs)}` : "",
    ].filter(Boolean);
    const staleMonitorWindows = [
      report30Freshness.stale ? `${monitor30Label} ${report30Freshness.ageLabel}` : "",
      report12Freshness.stale ? `${monitor12Label} ${report12Freshness.ageLabel}` : "",
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

    let title = t("opsGate.runtime.state.readyTitle");
    let detail = t("opsGate.runtime.state.readyDetail");
    if (level === "block") {
      title = t("opsGate.runtime.state.blockTitle");
      detail = String(report12.final_recommendation || report30.final_recommendation || t("opsGate.runtime.state.blockDetailFallback"));
    } else if (level === "escalate") {
      title = t("opsGate.runtime.state.escalateTitle");
      if (reconciliationStatus === "fail" || eodStatus === "fail") {
        const summary = (reconciliationIssues.length > 0 ? reconciliationIssues : eodIssues).slice(0, 2).join("; ");
        detail = t("opsGate.runtime.state.escalateFailDetail", {
          summarySuffix: summary ? t("opsGate.runtime.state.summarySuffix", { summary }) : "",
        });
      } else {
        detail = String(report12.final_recommendation || report30.final_recommendation || t("opsGate.runtime.state.escalateFallback"));
      }
    } else if (level === "observe") {
      title = t("opsGate.runtime.state.observeTitle");
      if (reconciliationStatus === "warn" || eodStatus === "warn") {
        const summary = (reconciliationIssues.length > 0 ? reconciliationIssues : eodIssues).slice(0, 2).join("; ");
        detail = t("opsGate.runtime.state.observeWarnDetail", {
          summarySuffix: summary ? t("opsGate.runtime.state.summarySuffix", { summary }) : "",
        });
      } else if (monitorFocus.length > 0) {
        detail = t("opsGate.runtime.state.observeMonitorDetail", { focus: monitorFocus.join(" / ") });
      } else if (staleMonitorWindows.length > 0) {
        detail = t("opsGate.runtime.state.observeStaleDetail", { windows: staleMonitorWindows.join(" / ") });
      } else {
        detail = t("opsGate.runtime.state.observeFallback");
      }
    }

    const items = [
      {
        label: t("opsGate.runtime.checks.startupLabel"),
        value: startup.ready === false
          ? t("opsGate.runtime.checks.startupCounts", {
            failures: Number(startup.failure_count || 0),
            warnings: Number(startup.warning_count || 0),
          })
          : startup.ready === true
            ? te("report_status", "ready", "ready")
            : te("report_status", "unknown", "unknown"),
      },
      {
        label: monitor30Label,
        value: monitorWindowDisplaySummary(report30, 30 * 60, nowTs),
      },
      {
        label: monitor12Label,
        value: monitorWindowDisplaySummary(report12, 12 * 60 * 60, nowTs),
      },
      {
        label: t("opsGate.runtime.checks.reconciliationLabel"),
        value: reconciliationStatusSummary(reconciliation, eod),
      },
    ];

    const issueParts = [];
    for (const row of checks.filter((item) => {
      const status = String(item && item.status || "").toUpperCase();
      return status === "FAIL" || status === "WARN";
    }).slice(0, 4)) {
      const status = String(row && row.status || "");
      issueParts.push(t("opsGate.runtime.checks.issueLine", {
        name: startupCheckNameLabel(row && row.name || ""),
        message: String(row && row.message || status),
      }));
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
          t("opsGate.runtime.actions.networkSmoke.title"),
          t("opsGate.runtime.actions.networkSmoke.detail", {
            message: message || t("opsGate.runtime.actions.networkSmoke.fallbackMessage"),
          }),
          [
            { type: "copy", label: t("opsGate.runtime.controls.copyCommand"), value: "make network-smoke" },
            { type: "open", label: t("opsGate.runtime.controls.openStateJson"), value: "/api/state" },
            { type: "jump", label: t("opsGate.runtime.controls.jumpDiagnostics"), value: "diagnostics-panel" },
          ]
        );
      } else if (name === "api_credentials" || name === "funder_address" || name === "signature_type") {
        pushAction(
          "danger",
          t("opsGate.runtime.actions.liveCredentials.title"),
          t("opsGate.runtime.actions.liveCredentials.detail", { message: message || startupCheckNameLabel(name) }),
          [
            { type: "open", label: t("opsGate.runtime.controls.openStateJson"), value: "/api/state" },
            { type: "jump", label: t("opsGate.runtime.controls.jumpDiagnostics"), value: "diagnostics-panel" },
          ]
        );
      } else if (name === "market_preflight") {
        pushAction(
          "danger",
          t("opsGate.runtime.actions.marketPreflight.title"),
          t("opsGate.runtime.actions.marketPreflight.detail", { message: message || startupCheckNameLabel(name) }),
          [
            { type: "open", label: t("opsGate.runtime.controls.openStateJson"), value: "/api/state" },
            { type: "jump", label: t("opsGate.runtime.controls.jumpDiagnostics"), value: "diagnostics-panel" },
          ]
        );
      } else if (name === "order_status_support") {
        pushAction(
          "danger",
          t("opsGate.runtime.actions.orderStatusSupport.title"),
          t("opsGate.runtime.actions.orderStatusSupport.detail", { message: message || startupCheckNameLabel(name) }),
          [
            { type: "open", label: t("opsGate.runtime.controls.openStateJson"), value: "/api/state" },
            { type: "jump", label: t("opsGate.runtime.controls.jumpDiagnostics"), value: "diagnostics-panel" },
          ]
        );
      } else if (name === "heartbeat_support") {
        if (pendingOrderCount > 0) {
          pushAction(
            "wait",
            t("opsGate.runtime.actions.heartbeat.title"),
            t("opsGate.runtime.actions.heartbeat.detail", {
              pendingCount: pendingOrderCount,
              message: message || startupCheckNameLabel(name),
            }),
            [
              { type: "open", label: t("opsGate.runtime.controls.openStateJson"), value: "/api/state" },
              { type: "jump", label: t("opsGate.runtime.controls.jumpDiagnostics"), value: "diagnostics-panel" },
            ]
          );
        }
      } else if (name === "user_stream") {
        pushAction(
          "wait",
          t("opsGate.runtime.actions.userStream.title"),
          t("opsGate.runtime.actions.userStream.detail", { message: message || startupCheckNameLabel(name) }),
          [
            { type: "copy", label: t("opsGate.runtime.controls.copyInstallCommand"), value: ".venv/bin/pip install websocket-client" },
            { type: "open", label: t("opsGate.runtime.controls.openStateJson"), value: "/api/state" },
            { type: "jump", label: t("opsGate.runtime.controls.jumpDiagnostics"), value: "diagnostics-panel" },
          ]
        );
      } else if (name === "clob_host") {
        pushAction(
          "danger",
          t("opsGate.runtime.actions.clobHost.title"),
          t("opsGate.runtime.actions.clobHost.detail", { message: message || startupCheckNameLabel(name) }),
          [
            { type: "open", label: t("opsGate.runtime.controls.openStateJson"), value: "/api/state" },
            { type: "jump", label: t("opsGate.runtime.controls.jumpDiagnostics"), value: "diagnostics-panel" },
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
        t("opsGate.runtime.actions.ledgerDrift.title"),
        t("opsGate.runtime.actions.ledgerDrift.detail", { diff: fmtUsd(internalDiff) }),
        [
          { type: "api", label: t("opsGate.runtime.controls.refreshEodReport"), value: "generate_reconciliation_report" },
          { type: "copy", label: t("opsGate.runtime.controls.copyCommand"), value: "make reconciliation-report" },
          { type: "open", label: t("opsGate.runtime.controls.openEodJson"), value: "/api/reconciliation/eod" },
          { type: "jump", label: t("opsGate.runtime.controls.jumpDiagnostics"), value: "diagnostics-panel" },
          { type: "jump", label: t("opsGate.runtime.controls.jumpMonitor"), value: "monitor-report-panel" },
        ]
      );
    }
    if (stalePending > 0) {
      pushAction(
        stalePending >= 2 ? "danger" : "wait",
        t("opsGate.runtime.actions.stalePending.title"),
        t("opsGate.runtime.actions.stalePending.detail", { count: stalePending }),
        [
          { type: "api", label: t("opsGate.runtime.controls.clearStalePending"), value: "clear_stale_pending" },
          { type: "jump", label: t("opsGate.runtime.controls.jumpOrders"), value: "orders-panel" },
          { type: "jump", label: t("opsGate.runtime.controls.jumpDiagnostics"), value: "diagnostics-panel" },
          { type: "jump", label: t("opsGate.runtime.controls.jumpMonitor"), value: "monitor-report-panel" },
        ]
      );
    }
    if (accountAge > 1800 || reconcileAge > 900 || eventAge > 900) {
      pushAction(
        "wait",
        t("opsGate.runtime.actions.syncAge.title"),
        t("opsGate.runtime.actions.syncAge.detail", {
          accountAge: fmtAge(accountAge),
          reconcileAge: fmtAge(reconcileAge),
          eventAge: fmtAge(eventAge),
        }),
        [
          { type: "jump", label: t("opsGate.runtime.controls.jumpDiagnostics"), value: "diagnostics-panel" },
          { type: "jump", label: t("opsGate.runtime.controls.jumpMonitor"), value: "monitor-report-panel" },
          { type: "open", label: t("opsGate.runtime.controls.openStateJson"), value: "/api/state" },
        ]
      );
    }

    if (inconclusiveWindows.length > 0) {
      const windowSummary = inconclusiveWindows
        .map((item) => t("opsGate.runtime.actions.inconclusive.windowPart", {
          label: item.label,
          count: item.count > 0 ? item.count : t("opsGate.runtime.actions.inconclusive.unknownCount"),
        }))
        .join(" / ");
      pushAction(
        "wait",
        t("opsGate.runtime.actions.inconclusive.title"),
        t("opsGate.runtime.actions.inconclusive.detail", { windows: windowSummary }),
        [
          { type: "jump", label: t("opsGate.runtime.controls.jumpDiagnostics"), value: "diagnostics-panel" },
          { type: "jump", label: t("opsGate.runtime.controls.jumpMonitor"), value: "monitor-report-panel" },
        ]
      );
    }

    if (report30RawKind !== "ready" || report12RawKind !== "ready" || report30Freshness.stale || report12Freshness.stale) {
      pushAction(
        level === "block" || level === "escalate" ? "danger" : "wait",
        t("opsGate.runtime.actions.monitorSummary.title"),
        t("opsGate.runtime.actions.monitorSummary.detail", {
          monitor30: monitorWindowDisplaySummary(report30, 30 * 60, nowTs),
          monitor12: monitorWindowDisplaySummary(report12, 12 * 60 * 60, nowTs),
        }),
        [
          { type: "jump", label: t("opsGate.runtime.controls.jumpDiagnostics"), value: "diagnostics-panel" },
          { type: "jump", label: t("opsGate.runtime.controls.jumpMonitor"), value: "monitor-report-panel" },
          { type: "open", label: t("opsGate.runtime.controls.open30mJson"), value: "/api/monitor/30m" },
          { type: "open", label: t("opsGate.runtime.controls.open12hJson"), value: "/api/monitor/12h" },
        ]
      );
    }

    if (actions.length === 0) {
      pushAction(
        "ok",
        t("opsGate.runtime.actions.consistency.title"),
        t("opsGate.runtime.actions.consistency.detail"),
        [
          { type: "api", label: t("opsGate.runtime.controls.refreshEodReport"), value: "generate_reconciliation_report" },
          { type: "jump", label: t("opsGate.runtime.controls.jumpDiagnostics"), value: "diagnostics-panel" },
          { type: "jump", label: t("opsGate.runtime.controls.jumpMonitor"), value: "monitor-report-panel" },
          { type: "open", label: t("opsGate.runtime.controls.openEodJson"), value: "/api/reconciliation/eod" },
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
      if (action === "trim") return ["trim", actionLabel || t("enum.actionTag.trim")];
      if (action === "exit") return ["exit", actionLabel || t("enum.actionTag.exit")];
      return ["exit", actionLabel || String(order && order.exit_label || t("enum.actionTag.exit"))];
    }
    if (action === "add") return ["add", actionLabel || t("enum.actionTag.add")];
    if (action === "entry") return ["entry", actionLabel || t("enum.actionTag.entry")];
    if (side === "BUY") return ["entry", actionLabel || sideLabel("BUY")];
    if (side === "SELL") return ["exit", actionLabel || sideLabel("SELL")];
    return [action || side.toLowerCase(), actionLabel || sideLabel(side) || t("enum.actionTag.event")];
  }

  function actionTagMeta(action, label) {
    const value = String(action || "").trim().toLowerCase();
    if (value === "entry") return ["ok", label || t("enum.actionTag.entry")];
    if (value === "add") return ["wait", label || t("enum.actionTag.add")];
    if (value === "trim") return ["cancel", label || t("enum.actionTag.trim")];
    if (value === "exit") return ["danger", label || t("enum.actionTag.exit")];
    return ["wait", label || t("enum.actionTag.event")];
  }

  function signalStatusMeta(status) {
    const value = String(status || "").trim().toLowerCase();
    if (value === "filled") return ["ok", t("enum.signalStatus.filled")];
    if (value === "risk_rejected") return ["danger", t("enum.signalStatus.risk_rejected")];
    if (value === "order_rejected") return ["danger", t("enum.signalStatus.order_rejected")];
    if (value === "duplicate_skipped") return ["cancel", t("enum.signalStatus.duplicate_skipped")];
    if (value === "skipped") return ["cancel", t("enum.signalStatus.skipped")];
    if (value === "candidate") return ["wait", t("enum.signalStatus.candidate")];
    return ["wait", t("enum.signalStatus.unknown")];
  }

  function traceStatusMeta(status) {
    const value = String(status || "").trim().toLowerCase();
    if (value === "closed") return ["cancel", t("enum.traceStatus.closed")];
    if (value === "pending") return ["wait", t("enum.traceStatus.pending")];
    if (value === "approved") return ["wait", t("enum.traceStatus.approved")];
    if (value === "executed") return ["ok", t("enum.traceStatus.executed")];
    return ["ok", t("enum.traceStatus.open")];
  }

  function sourceWalletLabel(value) {
    const raw = String(value || "").trim();
    if (!raw) return t("wallet.source.unlabeled");
    if (raw === "system-time-exit") return t("wallet.source.systemTimeExit");
    if (raw === "system-emergency-stop") return t("wallet.source.systemEmergencyStop");
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
    if (value === "filled" || value === "matched" || value === "confirmed") return ["ok", te("pending_order_status", "filled", t("orders.status.filled"))];
    if (value === "partially_filled" || value === "partial_fill" || value === "delayed") return ["wait", te("pending_order_status", value, t("orders.status.partiallyFilled"))];
    if (value === "live" || value === "pending" || value === "submitted" || value === "accepted") return ["wait", te("pending_order_status", value, t("orders.status.pending"))];
    if (value === "canceled" || value === "cancelled" || value === "unmatched") return ["cancel", te("pending_order_status", value, t("orders.status.canceled"))];
    if (value === "failed" || value === "rejected" || value === "error") return ["danger", te("pending_order_status", value, t("orders.status.failed"))];
    return ["cancel", te("pending_order_status", value || "unknown", t("common.unknown"))];
  }

  function operatorActionMeta(action) {
    const status = String(action && action.status || "").trim().toLowerCase();
    const cleared = Number(action && action.cleared_count || 0);
    if (status === "requested") return { cls: "wait", label: te("operator_action", status, t("orders.status.pending")), valueCls: "warn" };
    if (status === "cleared") {
      return {
        cls: "ok",
        label: cleared > 0
          ? t("diagnostics.runtime.operatorAction.clearedCount", { count: cleared })
          : te("operator_action", status, t("orders.status.cleared")),
        valueCls: "value-positive",
      };
    }
    if (status === "noop") return { cls: "cancel", label: te("operator_action", status, t("common.none")), valueCls: "value-neutral" };
    return { cls: "cancel", label: te("operator_action", "idle", t("common.waiting")), valueCls: "value-neutral" };
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
      labels.push(selected ? String(selected.label || t("exitReview.runtime.defaults.exit")) : exitReviewFilter.kind);
    }
    if (exitReviewFilter.topic) {
      const selected = byTopic.find((item) => String(item.topic_label || item.key || "") === exitReviewFilter.topic);
      labels.push(selected ? String(selected.label || t("candidate.focus.runtime.values.unlabeled")) : exitReviewFilter.topic);
    }
    if (exitReviewFilter.source) {
      const selected = bySource.find((item) => String(item.source_wallet || item.key || "") === exitReviewFilter.source);
      labels.push(selected ? sourceWalletLabel(selected.source_wallet || selected.key) : sourceWalletLabel(exitReviewFilter.source));
    }
    return labels.length > 0
      ? t("exitReview.filter.summary", { labels: labels.join(" / ") })
      : t("exitReview.filter.summaryEmpty");
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
      ? t("exitReview.panel.metaValue", { count: total, topics, sources })
      : t("exitReview.panel.meta");

    summaryEl.innerHTML = [
      `<div class="review-chip"><span>${escapeHtml(t("exitReview.summary.recentExit"))}</span><b>${total}</b></div>`,
      `<div class="review-chip"><span>${escapeHtml(t("exitReview.summary.filled"))}</span><b>${filled}</b></div>`,
      `<div class="review-chip"><span>${escapeHtml(t("exitReview.summary.rejected"))}</span><b>${rejected}</b></div>`,
      `<div class="review-chip"><span>${escapeHtml(t("exitReview.summary.amount"))}</span><b>${fmtUsd(totalNotional, false)}</b></div>`,
      `<div class="review-chip"><span>${escapeHtml(t("exitReview.summary.avgHold"))}</span><b>${avgHoldMinutes > 0 ? fmtHoldMinutes(avgHoldMinutes) : escapeHtml(t("common.unknownShort"))}</b></div>`,
      `<div class="review-chip"><span>${escapeHtml(t("exitReview.summary.maxHold"))}</span><b>${maxHoldMinutes > 0 ? fmtHoldMinutes(maxHoldMinutes) : escapeHtml(t("common.unknownShort"))}</b></div>`,
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
                <b>${escapeHtml(t("exitReview.runtime.fillReject", {
                  filled: Number(item.filled_count || 0),
                  rejected: Number(item.rejected_count || 0),
                }))}</b>
              </div>
              <div class="review-sub">${detail}</div>
            </li>`;
          }).join("")
        : `<li><div class="review-main"><span>${emptyText}</span><b>--</b></div></li>`;
    };

    renderReviewList(
      kindEl,
      review && review.by_kind,
      (item) => String(item.label || t("exitReview.runtime.defaults.kind")),
      (item) => t("exitReview.runtime.reviewList.detail", {
        notional: fmtUsd(item.notional || 0, false),
        count: Number(item.count || 0),
        avgHold: Number(item.avg_hold_minutes || 0) > 0 ? fmtHoldMinutes(item.avg_hold_minutes) : t("common.unknownShort"),
        latest: Number(item.latest_ts || 0) > 0 ? historyAgeLabel(item.latest_ts, now) : t("common.notRecorded"),
      }),
      t("exitReview.runtime.empty.byKind"),
      "kind",
      (item) => String(item.exit_kind || item.key || "")
    );
    renderReviewList(
      topicEl,
      review && review.by_topic,
      (item) => String(item.label || t("attributionReview.runtime.defaults.topic")),
      (item) => t("exitReview.runtime.reviewList.detail", {
        notional: fmtUsd(item.notional || 0, false),
        count: Number(item.count || 0),
        avgHold: Number(item.avg_hold_minutes || 0) > 0 ? fmtHoldMinutes(item.avg_hold_minutes) : t("common.unknownShort"),
        latest: Number(item.latest_ts || 0) > 0 ? historyAgeLabel(item.latest_ts, now) : t("common.notRecorded"),
      }),
      t("exitReview.runtime.empty.byTopic"),
      "topic",
      (item) => String(item.topic_label || item.key || "")
    );
    renderReviewList(
      sourceEl,
      review && review.by_source,
      (item) => sourceWalletLabel(item.source_wallet || item.key),
      (item) => t("exitReview.runtime.reviewList.detail", {
        notional: fmtUsd(item.notional || 0, false),
        count: Number(item.count || 0),
        avgHold: Number(item.avg_hold_minutes || 0) > 0 ? fmtHoldMinutes(item.avg_hold_minutes) : t("common.unknownShort"),
        latest: Number(item.latest_ts || 0) > 0 ? historyAgeLabel(item.latest_ts, now) : t("common.notRecorded"),
      }),
      t("exitReview.runtime.empty.bySource"),
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
    samplesMetaEl.textContent = t("exitReview.samples.metaValue", { visible: filteredSamples.length, total: samples.length });
    samplesBodyEl.innerHTML = filteredSamples.length > 0
      ? filteredSamples.map((sample) => {
          const sampleKey = exitSampleKey(sample);
          const [exitCls, exitTag] = exitTagMeta(sample.exit_kind, sample.exit_label);
          const [resultCls, resultTag] = exitResultMeta(sample.exit_result, sample.exit_result_label);
          const status = String(sample.status || "").toUpperCase();
          const [statusCls, statusText] = orderResultStatusMeta(status);
          const summaryText = String(sample.exit_summary || "").trim();
          return `<tr>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main mono">${hhmm(Number(sample.ts || now))}</span>
                <span class="cell-sub">${Number(sample.ts || 0) > 0 ? historyAgeLabel(sample.ts, now) : t("common.notRecorded")}</span>
              </div>
            </td>
            <td>${sample.title || "-"}</td>
            <td class="wrap">
              <div class="cell-stack">
                <span><span class="tag ${exitCls}">${exitTag}</span> <span class="tag ${resultCls}">${resultTag}</span> <span class="tag ${statusCls}">${statusText}</span></span>
                <span class="cell-sub">${t("exitReview.runtime.sample.statusDetail", {
                  kind: String(sample.exit_kind || t("exitReview.runtime.defaults.exit")),
                  hold: Number(sample.hold_minutes || 0) > 0 ? fmtHoldMinutes(sample.hold_minutes) : t("common.unknownShort"),
                })}</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${sourceWalletLabel(sample.source_wallet || sample.source_label)}</span>
                <span class="cell-sub">${sample.topic_label || t("attributionReview.runtime.defaults.topic")}</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${fmtUsd(sample.notional || 0, false)}</span>
                <span class="cell-sub">${summaryText || t("exitReview.runtime.sample.summaryEmpty")}</span>
              </div>
            </td>
          </tr>`.replace("<tr>", `<tr class="click-row${sampleKey === selectedExitSampleKey ? " active-row" : ""}" data-exit-sample-key="${attrToken(sampleKey)}" data-trace-id="${attrToken(sample.trace_id || sample.current_position && sample.current_position.trace_id || "")}">`);
        }).join("")
      : `<tr><td colspan="5">${escapeHtml(t("exitReview.runtime.empty.filteredSamples"))}</td></tr>`;

    const selectedSample = filteredSamples.find((sample) => exitSampleKey(sample) === selectedExitSampleKey) || filteredSamples[0] || null;
    if (!selectedSample) {
      detailMetaEl.textContent = t("exitReview.detail.meta");
      detailHeadEl.innerHTML = `<span class="tag danger">${escapeHtml(t("common.waiting"))}</span><span class="mono">${escapeHtml(t("exitReview.detail.hint"))}</span>`;
      detailSummaryEl.textContent = t("exitReview.detail.summary");
      detailListEl.innerHTML = `<li><span>${escapeHtml(t("exitReview.detail.list.status"))}</span><b>${escapeHtml(t("common.waitingData"))}</b></li>`;
      detailChainMetaEl.textContent = t("exitReview.detail.chain.meta");
      detailChainEl.innerHTML = `<li><span>${escapeHtml(t("common.dashTime"))}</span><b>${escapeHtml(t("common.waitingData"))}</b></li>`;
    } else {
      const [exitCls, exitTag] = exitTagMeta(selectedSample.exit_kind, selectedSample.exit_label);
      const [resultCls, resultTag] = exitResultMeta(selectedSample.exit_result, selectedSample.exit_result_label);
      const status = String(selectedSample.status || "").toUpperCase();
      const [statusCls, statusText] = orderResultStatusMeta(status);
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
      detailMetaEl.textContent = t("exitReview.runtime.detail.metaValue", {
        time: hhmm(Number(selectedSample.ts || now)),
        tag: exitTag,
      });
      detailHeadEl.innerHTML =
        `<span class="tag ${exitCls}">${exitTag}</span>` +
        `<span class="tag ${resultCls}">${resultTag}</span>` +
        `<span class="tag ${statusCls}">${statusText}</span>` +
        `<span class="tag ${currentOpen ? "wait" : "cancel"}">${currentOpen ? t("exitReview.runtime.detail.currentOpen") : t("exitReview.runtime.detail.currentClosed")}</span>`;
      detailSummaryEl.textContent = detailSummary || t("exitReview.runtime.detail.summaryEmpty");
      detailListEl.innerHTML = [
        `<li><span>${escapeHtml(t("exitReview.runtime.detail.list.marketDirection"))}</span><b>${selectedSample.title || "-"} · ${selectedSample.outcome || "--"}</b></li>`,
        `<li><span>${escapeHtml(t("exitReview.runtime.detail.list.sourceWallet"))}</span><b>${sourceWalletLabel(selectedSample.source_wallet || selectedSample.source_label)}${selectedSample.wallet_tier ? ` · ${walletTierLabel(selectedSample.wallet_tier)}` : ""}${Number(selectedSample.wallet_score || 0) > 0 ? ` · ${Number(selectedSample.wallet_score || 0).toFixed(1)}` : ""}</b></li>`,
        `<li><span>${escapeHtml(t("exitReview.runtime.detail.list.entryContext"))}</span><b>${entryWallet ? `${sourceWalletLabel(entryWallet)}${entryTier ? ` · ${walletTierLabel(entryTier)}` : ""}${entryScore > 0 ? ` · ${entryScore.toFixed(1)}` : ""}` : t("exitReview.runtime.detail.entryWalletEmpty")}${entryTopic ? ` · ${entryTopic}` : ""}</b></li>`,
        `<li><span>${escapeHtml(t("exitReview.runtime.detail.list.entryReason"))}</span><b>${entryReason || entryTopicSummary || t("exitReview.runtime.detail.entryReasonEmpty")}</b></li>`,
        `<li><span>${escapeHtml(t("exitReview.runtime.detail.list.trace"))}</span><b>${selectedSample.trace_id || position.trace_id || "--"}</b></li>`,
        `<li><span>${escapeHtml(t("exitReview.runtime.detail.list.holdMinutes"))}</span><b>${Number(selectedSample.hold_minutes || 0) > 0 ? fmtHoldMinutes(selectedSample.hold_minutes) : t("common.unknownShort")}</b></li>`,
        `<li><span>${escapeHtml(t("exitReview.runtime.detail.list.currentPosition"))}</span><b>${currentOpen ? `${fmtUsd(position.notional || 0, false)} / ${Number(position.quantity || 0).toFixed(2)}${t("common.units.shares")}` : t("exitReview.runtime.detail.currentPositionClosed")}</b></li>`,
        `<li><span>${escapeHtml(t("exitReview.runtime.detail.list.latestExit"))}</span><b>${position.last_exit_label ? `${position.last_exit_label}${position.last_exit_summary ? ` · ${position.last_exit_summary}` : ""}` : detailSummary || t("exitReview.runtime.detail.latestExitEmpty")}</b></li>`,
      ].join("");
      detailChainMetaEl.textContent = t("exitReview.runtime.detail.chainMetaValue", { count: eventChain.length });
      detailChainEl.innerHTML = eventChain.length > 0
        ? eventChain.map((item) => {
          const [itemCls, itemTag] = item.flow === "exit"
              ? exitTagMeta(item.exit_kind || "", item.action_label || "")
              : ["ok", item.action_label || t("exitReview.runtime.event.buy")];
          const [itemResultCls, itemResultTag] = item.flow === "exit"
              ? exitResultMeta(item.exit_result, item.exit_result_label)
              : ["ok", t("exitReview.runtime.event.entry")];
          return `<li>
              <span>${hhmm(Number(item.ts || now))}</span>
              <b><span class="tag ${itemCls}">${itemTag}</span> <span class="tag ${itemResultCls}">${itemResultTag}</span> ${fmtUsd(item.notional || 0, false)}${Number(item.hold_minutes || 0) > 0 ? ` · ${t("exitReview.runtime.event.hold", { hold: fmtHoldMinutes(item.hold_minutes) })}` : ""}</b>
              <div class="cell-sub">${String(item.reason || t("exitReview.runtime.event.reasonEmpty"))}</div>
            </li>`;
          }).join("")
        : `<li><span>${escapeHtml(t("common.dashTime"))}</span><b>${escapeHtml(t("exitReview.runtime.detail.chainEmpty"))}</b></li>`;
    }

    if (total <= 0 && latestExitTs <= 0) {
      metaEl.textContent = t("exitReview.panel.meta");
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
    metaEl.textContent = t("traceReview.panel.metaValue", {
      cycles: Number(summary.cycles || 0),
      traces: Number(summary.traces || 0),
    });
    summaryEl.innerHTML = [
      `<div class="review-chip"><span>${escapeHtml(t("traceReview.summary.recentCycles"))}</span><b>${Number(summary.cycles || 0)}</b></div>`,
      `<div class="review-chip"><span>${escapeHtml(t("traceReview.summary.candidates"))}</span><b>${Number(summary.candidates || 0)}</b></div>`,
      `<div class="review-chip"><span>${escapeHtml(t("traceReview.summary.filled"))}</span><b>${Number(summary.filled || 0)}</b></div>`,
      `<div class="review-chip"><span>${escapeHtml(t("traceReview.summary.rejected"))}</span><b>${Number(summary.rejected || 0)}</b></div>`,
      `<div class="review-chip"><span>${escapeHtml(t("traceReview.summary.activeTraces"))}</span><b>${Number(summary.open_traces || 0)}</b></div>`,
      `<div class="review-chip"><span>${escapeHtml(t("traceReview.summary.closedTraces"))}</span><b>${Number(summary.closed_traces || 0)}</b></div>`,
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
            : t("traceReview.runtime.cycles.previewEmpty");
          return `<li class="review-selectable${active ? " active" : ""}" data-signal-cycle-id="${attrToken(cycle.cycle_id || "")}">
            <div class="review-main">
              <span>${hhmm(Number(cycle.ts || now))}</span>
              <b>${escapeHtml(t("traceReview.runtime.cycles.countValue", {
                count: Number(cycle.candidate_count || 0),
              }))}</b>
            </div>
            <div class="review-sub">${escapeHtml(t("traceReview.runtime.cycles.detail", {
              filled: Number(cycle.filled_count || 0),
              rejected: Number(cycle.rejected_count || 0),
              skipped: Number(cycle.skipped_count || 0),
              preview: previewText,
            }))}</div>
          </li>`;
        }).join("")
      : `<li><div class="review-main"><span>${escapeHtml(t("traceReview.runtime.empty.cycles"))}</span><b>${escapeHtml(t("common.dash"))}</b></div></li>`;

    const cycleCandidates = Array.isArray(selectedCycle && selectedCycle.candidates) ? selectedCycle.candidates : [];
    cycleMetaEl.textContent = t("traceReview.columns.cycleCandidatesMetaValue", { count: cycleCandidates.length });
    cycleBodyEl.innerHTML = cycleCandidates.length > 0
      ? cycleCandidates.map((candidate) => {
          const [actionCls, actionTag] = actionTagMeta(candidate.action, candidate.action_label);
          const [statusCls, statusTag] = signalStatusMeta(candidate.final_status);
          const walletPoolPreview = Array.isArray(candidate.wallet_pool_preview) ? candidate.wallet_pool_preview : [];
          const previewText = walletPoolPreview.length > 0
            ? walletPoolPreview.map((item) => `${shortWallet(item.wallet)} ${Number(item.wallet_score || 0).toFixed(0)}`).join(" / ")
            : t("traceReview.runtime.candidates.previewEmpty");
          const traceId = String(candidate.trace_id || "");
          const rowClass = traceId && traceId === selectedTraceId ? "click-row active-row" : "click-row";
          const decisionReason = String(candidate.decision_reason || "").trim();
          const orderReason = String(candidate.order_reason || "").trim();
          return `<tr>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${candidate.title || "-"}</span>
                <span class="cell-sub">${candidate.outcome || "--"} · ${candidate.topic_label || t("attributionReview.runtime.defaults.topic")}</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${shortWallet(candidate.wallet)}</span>
                <span class="cell-sub">${candidate.wallet_tier ? walletTierLabel(candidate.wallet_tier) : "-"} · ${Number(candidate.wallet_score || 0).toFixed(1)}</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span><span class="tag ${actionCls}">${actionTag}</span> <span class="tag ${statusCls}">${statusTag}</span></span>
                <span class="cell-sub">${candidate.topic_bias || t("traceReview.runtime.candidates.topicBiasNeutral")} x${Number(candidate.topic_multiplier || 1).toFixed(2)}</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${decisionReason || t("traceReview.runtime.candidates.decisionPass")}</span>
                <span class="cell-sub">${Number(candidate.final_notional || 0) > 0 ? `${fmtUsd(candidate.final_notional || 0, false)} / ` : ""}${previewText}</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${orderReason || (candidate.order_status || t("traceReview.runtime.candidates.noOrder"))}</span>
                <span class="cell-sub">${candidate.order_status || t("traceReview.runtime.candidates.candidateState")}${Number(candidate.order_notional || 0) > 0 ? ` · ${fmtUsd(candidate.order_notional || 0, false)}` : ""}</span>
              </div>
            </td>
          </tr>`.replace("<tr>", `<tr class="${rowClass}" data-trace-id="${attrToken(traceId)}" data-signal-cycle-id="${attrToken(selectedCycle && selectedCycle.cycle_id || "")}">`);
        }).join("")
      : `<tr><td colspan="5">${escapeHtml(t("traceReview.runtime.empty.cycleCandidates"))}</td></tr>`;

    if (!selectedTrace) {
      detailMetaEl.textContent = t("traceReview.detail.meta");
      detailHeadEl.innerHTML = `<span class="tag danger">${escapeHtml(t("common.waiting"))}</span><span class="mono">${escapeHtml(t("traceReview.detail.hint"))}</span>`;
      detailSummaryEl.textContent = t("traceReview.detail.summary");
      detailListEl.innerHTML = `<li><span>${escapeHtml(t("traceReview.detail.list.status"))}</span><b>${escapeHtml(t("common.waitingData"))}</b></li>`;
      detailChainMetaEl.textContent = t("traceReview.detail.chain.meta");
      detailChainEl.innerHTML = `<li><span>${escapeHtml(t("common.dashTime"))}</span><b>${escapeHtml(t("common.waitingData"))}</b></li>`;
      persistExitReviewUiState();
      return;
    }

    const [traceCls, traceTag] = traceStatusMeta(selectedTrace.status);
    const currentPosition = selectedTrace.current_position || {};
    const decisionChain = Array.isArray(selectedTrace.decision_chain) ? selectedTrace.decision_chain : [];
    const latestStep = decisionChain[decisionChain.length - 1] || {};
    const [latestActionCls, latestActionTag] = actionTagMeta(selectedTrace.latest_action || latestStep.action, selectedTrace.latest_action_label || latestStep.action_label);
    const latestStatus = String(selectedTrace.latest_order_status || latestStep.order_status || "").toUpperCase();
    const [latestStatusCls, latestStatusTag] = latestStatus
      ? orderResultStatusMeta(latestStatus)
      : ["cancel", t("traceReview.runtime.detail.noOrder")];
    detailMetaEl.textContent = t("traceReview.runtime.detail.metaValue", {
      traceId: selectedTrace.trace_id || t("common.entity.trace"),
      market: selectedTrace.market_slug || t("common.dash"),
    });
    detailHeadEl.innerHTML =
      `<span class="tag ${traceCls}">${traceTag}</span>` +
      `<span class="tag ${latestActionCls}">${latestActionTag}</span>` +
      `<span class="tag ${latestStatusCls}">${latestStatusTag}</span>` +
      `<span class="mono">${selectedTrace.entry_signal_id || "--"} -> ${selectedTrace.last_signal_id || "--"}</span>`;
    detailSummaryEl.textContent = String(selectedTrace.entry_reason || latestStep.order_reason || latestStep.skip_reason || latestStep.risk_reason || t("traceReview.runtime.detail.entryReasonEmpty"));
    detailListEl.innerHTML = [
      `<li><span>${escapeHtml(t("traceReview.runtime.detail.list.marketDirection"))}</span><b>${selectedTrace.market_slug || "-"} · ${selectedTrace.outcome || "--"}</b></li>`,
      `<li><span>${escapeHtml(t("traceReview.runtime.detail.list.firstEntry"))}</span><b>${selectedTrace.entry_wallet ? `${sourceWalletLabel(selectedTrace.entry_wallet)}${selectedTrace.entry_wallet_tier ? ` · ${walletTierLabel(selectedTrace.entry_wallet_tier)}` : ""}${Number(selectedTrace.entry_wallet_score || 0) > 0 ? ` · ${Number(selectedTrace.entry_wallet_score || 0).toFixed(1)}` : ""}` : t("traceReview.runtime.detail.entryWalletEmpty")}${selectedTrace.entry_topic_label ? ` · ${selectedTrace.entry_topic_label}` : ""}</b></li>`,
      `<li><span>${escapeHtml(t("traceReview.runtime.detail.list.currentPosition"))}</span><b>${currentPosition && currentPosition.trace_id ? `${fmtUsd(currentPosition.notional || 0, false)} / ${Number(currentPosition.quantity || 0).toFixed(2)}${t("common.units.shares")}` : t("traceReview.runtime.detail.currentPositionEmpty")}</b></li>`,
      `<li><span>${escapeHtml(t("traceReview.runtime.detail.list.latestAction"))}</span><b>${latestActionTag}${latestStep.order_reason ? ` · ${latestStep.order_reason}` : latestStep.skip_reason ? ` · ${latestStep.skip_reason}` : latestStep.risk_reason ? ` · ${latestStep.risk_reason}` : ""}</b></li>`,
      `<li><span>${escapeHtml(t("traceReview.runtime.detail.list.openedTime"))}</span><b>${Number(selectedTrace.opened_ts || 0) > 0 ? hhmm(selectedTrace.opened_ts) : "--"}${Number(selectedTrace.closed_ts || 0) > 0 ? ` / ${t("traceReview.runtime.detail.closedAt")} ${hhmm(selectedTrace.closed_ts)}` : ""}</b></li>`,
      `<li><span>${escapeHtml(t("traceReview.runtime.detail.list.currentStatus"))}</span><b>${traceTag}${currentPosition && currentPosition.last_exit_label ? ` · ${currentPosition.last_exit_label}` : ""}</b></li>`,
    ].join("");
    detailChainMetaEl.textContent = t("traceReview.runtime.detail.chainMetaValue", { count: decisionChain.length });
    detailChainEl.innerHTML = decisionChain.length > 0
      ? decisionChain.map((item) => {
          const [actionCls, actionTag] = actionTagMeta(item.action, item.action_label);
          const [statusCls, statusTag] = signalStatusMeta(item.final_status || item.order_status);
          const reasonText = String(item.order_reason || item.skip_reason || item.risk_reason || "").trim();
          const snapshotText = Number(item.position_notional || 0) > 0
            ? `${fmtUsd(item.position_notional || 0, false)} / ${Number(item.position_quantity || 0).toFixed(2)}${t("common.units.shares")}`
            : `${fmtUsd(item.final_notional || 0, false)}`;
          const poolPreview = Array.isArray(item.wallet_pool_preview) && item.wallet_pool_preview.length > 0
            ? item.wallet_pool_preview.map((row) => `${shortWallet(row.wallet)} ${Number(row.wallet_score || 0).toFixed(0)}`).join(" / ")
            : "";
          return `<li>
            <span>${Number(item.ts || 0) > 0 ? hhmm(item.ts) : "--:--"} · ${item.cycle_id || "--"} · ${item.signal_id || "--"}</span>
            <b><span class="tag ${actionCls}">${actionTag}</span> <span class="tag ${statusCls}">${statusTag}</span> ${snapshotText}</b>
            <div class="cell-sub">${item.wallet ? `${shortWallet(item.wallet)} · ` : ""}${item.topic_label ? `${item.topic_label} · ` : ""}${reasonText || t("traceReview.runtime.detail.chainReasonEmpty")}${poolPreview ? ` · ${t("traceReview.runtime.detail.poolPreview", { preview: poolPreview })}` : ""}</div>
          </li>`;
        }).join("")
      : `<li><span>${escapeHtml(t("common.dashTime"))}</span><b>${escapeHtml(t("traceReview.runtime.detail.chainEmpty"))}</b></li>`;

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
    const topicFallback = t("attributionReview.runtime.defaults.topic");
    const exitFallback = t("attributionReview.runtime.defaults.exit");
    const rejectFallback = t("attributionReview.runtime.defaults.reject");
    const holdBucketFallback = t("attributionReview.runtime.defaults.holdBucket");
    const formatHold = (minutes) => Number(minutes || 0) > 0 ? fmtHoldMinutes(minutes) : t("common.dash");
    const rowsMeta = (count) => t("attributionReview.runtime.table.metaRows", { count });

    metaEl.textContent = t("attributionReview.panel.metaValue", {
      orders: Number(summary.available_orders || 0),
      exits: Number(summary.available_exits || 0),
    });
    windowMetaEl.textContent = t("attributionReview.window.metaValue", {
      label: String(current.label || selectedAttributionWindow),
      orders: Number(currentSummary.order_count || 0),
    });

    Array.from(chipsEl.querySelectorAll("[data-window]")).forEach((el) => {
      const isActive = String(el.getAttribute("data-window") || "") === selectedAttributionWindow;
      el.classList.toggle("active", isActive);
    });

    summaryEl.innerHTML = [
      `<div class="review-chip"><span>${t("attributionReview.summary.orders")}</span><b>${Number(currentSummary.order_count || 0)}</b></div>`,
      `<div class="review-chip"><span>${t("attributionReview.summary.filled")}</span><b>${Number(currentSummary.filled_count || 0)}</b></div>`,
      `<div class="review-chip"><span>${t("attributionReview.summary.rejected")}</span><b>${Number(currentSummary.rejected_count || 0)}</b></div>`,
      `<div class="review-chip"><span>${t("attributionReview.summary.exits")}</span><b>${Number(currentSummary.exit_count || 0)}</b></div>`,
      `<div class="review-chip"><span>${t("attributionReview.summary.highScoreRejects")}</span><b>${Number(currentSummary.reject_high_score_count || 0)}</b></div>`,
      `<div class="review-chip"><span>${t("attributionReview.summary.topics")}</span><b>${Number(currentSummary.topics || 0)}</b></div>`,
    ].join("");

    const renderAttrList = (el, items, labelFn, detailFn, emptyText) => {
      const rows = Array.isArray(items) ? items : [];
      el.innerHTML = rows.length > 0
        ? rows.map((item) => `<li><div class="review-main"><span>${labelFn(item)}</span><b>${t("attributionReview.runtime.list.fillRejectShort", {
          filled: Number(item.filled_count || 0),
          rejected: Number(item.rejected_count || 0),
        })}</b></div><div class="review-sub">${detailFn(item)}</div></li>`).join("")
        : `<li><div class="review-main"><span>${emptyText}</span><b>--</b></div></li>`;
    };

    renderAttrList(
      byWalletEl,
      current.by_wallet,
      (item) => sourceWalletLabel(item.wallet || item.label),
      (item) => t("attributionReview.runtime.list.walletDetail", {
        entryCount: Number(item.entry_count || 0),
        exitCount: Number(item.exit_count || 0),
        notional: fmtUsd(item.filled_notional || 0, false),
        rejectRate: fmtRatioPct(item.reject_rate || 0, 0),
      }),
      t("attributionReview.runtime.empty.byWallet")
    );
    renderAttrList(
      byTopicEl,
      current.by_topic,
      (item) => String(item.topic_label || item.label || topicFallback),
      (item) => t("attributionReview.runtime.list.topicDetail", {
        entryCount: Number(item.entry_count || 0),
        exitCount: Number(item.exit_count || 0),
        notional: fmtUsd(item.filled_notional || 0, false),
        hold: formatHold(item.avg_hold_minutes),
      }),
      t("attributionReview.runtime.empty.byTopic")
    );
    renderAttrList(
      byExitKindEl,
      current.by_exit_kind,
      (item) => item.exit_kind
        ? te("exit_kind", item.exit_kind, String(item.label || exitFallback))
        : String(item.label || exitFallback),
      (item) => t("attributionReview.runtime.list.exitDetail", {
        filled: Number(item.filled_count || 0),
        rejected: Number(item.rejected_count || 0),
        notional: fmtUsd(item.filled_notional || 0, false),
        hold: formatHold(item.avg_hold_minutes),
      }),
      t("attributionReview.runtime.empty.byExitKind")
    );

    const walletTopicRows = Array.isArray(current.wallet_topic) ? current.wallet_topic : [];
    walletTopicMetaEl.textContent = rowsMeta(walletTopicRows.length);
    walletTopicBodyEl.innerHTML = walletTopicRows.length > 0
      ? walletTopicRows.map((row) => `<tr>
          <td>${sourceWalletLabel(row.wallet || row.label)}</td>
          <td>${row.topic_label || topicFallback}</td>
          <td>${Number(row.filled_count || 0)} / ${Number(row.rejected_count || 0)}</td>
          <td>${fmtUsd(row.filled_notional || 0, false)}</td>
          <td>${formatHold(row.avg_hold_minutes)}</td>
        </tr>`).join("")
      : `<tr><td colspan="5">${t("attributionReview.runtime.table.walletTopicEmpty")}</td></tr>`;

    const topicExitRows = Array.isArray(current.topic_exit) ? current.topic_exit : [];
    topicExitMetaEl.textContent = rowsMeta(topicExitRows.length);
    topicExitBodyEl.innerHTML = topicExitRows.length > 0
      ? topicExitRows.map((row) => `<tr>
          <td>${row.topic_label || topicFallback}</td>
          <td>${row.exit_kind ? te("exit_kind", row.exit_kind, String(row.exit_label || row.label || exitFallback)) : (row.exit_label || row.label || exitFallback)}</td>
          <td>${Number(row.filled_count || 0)} / ${Number(row.rejected_count || 0)}</td>
          <td>${fmtUsd(row.filled_notional || 0, false)}</td>
          <td>${formatHold(row.avg_hold_minutes)}</td>
        </tr>`).join("")
      : `<tr><td colspan="5">${t("attributionReview.runtime.table.topicExitEmpty")}</td></tr>`;

    const sourceResultRows = Array.isArray(current.source_result) ? current.source_result : [];
    sourceResultMetaEl.textContent = rowsMeta(sourceResultRows.length);
    sourceResultBodyEl.innerHTML = sourceResultRows.length > 0
      ? sourceResultRows.map((row) => `<tr>
          <td>${sourceWalletLabel(row.source_wallet || row.label)}</td>
          <td>${row.result_label || row.label || t("common.dash")}</td>
          <td>${Number(row.count || 0)}</td>
          <td>${fmtUsd(row.filled_notional || 0, false)}</td>
          <td>${fmtRatioPct(row.reject_rate || 0, 0)}</td>
        </tr>`).join("")
      : `<tr><td colspan="5">${t("attributionReview.runtime.table.sourceResultEmpty")}</td></tr>`;

    renderAttrList(
      rejectReasonsEl,
      current.reject_reasons,
      (item) => String(item.reason_label || item.label || rejectFallback),
      (item) => t("attributionReview.runtime.list.rejectDetail", {
        rejected: Number(item.rejected_count || 0),
        highScoreRejected: Number(item.high_score_rejected_count || 0),
        walletScore: Number(item.avg_wallet_score || 0).toFixed(1),
      }),
      t("attributionReview.runtime.empty.rejectReasons")
    );
    renderAttrList(
      holdBucketsEl,
      current.hold_buckets,
      (item) => String(item.hold_label || item.label || holdBucketFallback),
      (item) => t("attributionReview.runtime.list.holdBucketDetail", {
        count: Number(item.count || 0),
        avgHold: formatHold(item.avg_hold_minutes),
        maxHold: formatHold(item.max_hold_minutes),
      }),
      t("attributionReview.runtime.empty.holdBuckets")
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
      (row) => t("attributionReview.runtime.list.rankTopDetail", {
        filled: Number(row.filled_count || 0),
        notional: fmtUsd(row.filled_notional || 0, false),
      }),
      t("attributionReview.runtime.empty.rankings")
    );
    renderRankList(
      bottomWalletsEl,
      rankings.bottom_wallets,
      (row) => sourceWalletLabel(row.wallet || row.label),
      (row) => t("attributionReview.runtime.list.rankBottomDetail", {
        rejected: Number(row.rejected_count || 0),
        rejectRate: fmtRatioPct(row.reject_rate || 0, 0),
      }),
      t("attributionReview.runtime.empty.rankings")
    );
    renderRankList(
      topTopicsEl,
      rankings.top_topics,
      (row) => String(row.topic_label || row.label || topicFallback),
      (row) => t("attributionReview.runtime.list.rankTopDetail", {
        filled: Number(row.filled_count || 0),
        notional: fmtUsd(row.filled_notional || 0, false),
      }),
      t("attributionReview.runtime.empty.rankings")
    );
    renderRankList(
      bottomTopicsEl,
      rankings.bottom_topics,
      (row) => String(row.topic_label || row.label || topicFallback),
      (row) => t("attributionReview.runtime.list.rankBottomDetail", {
        rejected: Number(row.rejected_count || 0),
        rejectRate: fmtRatioPct(row.reject_rate || 0, 0),
      }),
      t("attributionReview.runtime.empty.rankings")
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
      ready: { cls: "ok", label: te("ops_gate_level", "ready", t("enum.opsGateLevel.ready")), dot: "dot-ok", guard: "guard-ok" },
      observe: { cls: "wait", label: te("ops_gate_level", "observe", t("enum.opsGateLevel.observe")), dot: "dot-wait", guard: "guard-wait" },
      escalate: { cls: "danger", label: te("ops_gate_level", "escalate", t("enum.opsGateLevel.escalate")), dot: "dot-danger", guard: "guard-danger" },
      block: { cls: "danger", label: te("ops_gate_level", "block", t("enum.opsGateLevel.block")), dot: "dot-danger", guard: "guard-danger" },
    };
    const meta = levelMap[level] || levelMap.observe;

    bannerEl.classList.remove("ops-ok", "ops-wait", "ops-danger");
    bannerEl.classList.add(level === "ready" ? "ops-ok" : level === "observe" ? "ops-wait" : "ops-danger");

    tagEl.className = `tag ${meta.cls}`;
    tagEl.textContent = meta.label;
    titleEl.textContent = String(gate && gate.title || t("opsGate.titleFallback"));
    detailEl.textContent = String(gate && gate.detail || t("opsGate.detailFallback"));

    const items = Array.isArray(gate && gate.items) ? gate.items : [];
    const issues = Array.isArray(gate && gate.issues) ? gate.issues : [];
    const actions = Array.isArray(gate && gate.actions) ? gate.actions : [];
    const visibleActions = actions.slice(0, 5);
    checksEl.innerHTML = items.length > 0
      ? items.map((item) => `<li><span>${String(item.label || "-")}</span><b>${String(item.value || "-")}</b></li>`).join("")
      : `<li><span>${t("common.statusLabel")}</span><b>${t("common.waitingData")}</b></li>`;
    if (issues.length > 0) {
      checksEl.innerHTML += issues.slice(0, 2).map((item) => `<li><span>${t("opsGate.issuesLabel")}</span><b>${String(item)}</b></li>`).join("");
    }

    actionsMetaEl.textContent = actions.length > visibleActions.length
      ? t("opsGate.actionsMeta", { visible: visibleActions.length, total: actions.length })
      : t("opsGate.actionsMetaSimple", { count: actions.length });
    actionsEl.innerHTML = visibleActions.length > 0
      ? visibleActions.map((item) => {
          const cls = String(item && item.cls || "wait");
          const tag = cls === "danger" ? t("opsGate.levelTag.danger") : cls === "ok" ? t("opsGate.levelTag.ok") : t("opsGate.levelTag.wait");
          const controls = Array.isArray(item && item.controls) ? item.controls : [];
          const controlsHtml = controls.length > 0
            ? `<div class="ops-action-buttons">${controls.map((control) => `<button class="btn ghost" data-ops-action="${String(control.type || "")}" data-ops-value="${attrToken(control.value || "")}">${String(control.label || "")}</button>`).join("")}</div>`
            : "";
          return `<li><span class="tag ${cls}">${tag}</span><div class="ops-action-body"><b>${String(item && item.title || "-")}</b><p>${String(item && item.detail || "")}</p>${controlsHtml}</div></li>`;
        }).join("")
      : `<li><span class="tag wait">${t("common.waiting")}</span><div><b>${t("opsGate.emptyActionTitle")}</b><p>${t("opsGate.emptyActionDetail")}</p></div></li>`;

    pillEl.className = `guard ${meta.guard}`;
    pillEl.textContent = t("opsGate.pillLabel", { label: meta.label });
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
    const issues = Array.isArray(eod.issue_labels) && eod.issue_labels.length > 0
      ? eod.issue_labels.filter((item) => String(item || "").trim())
      : Array.isArray(eod.issues)
        ? eod.issues.filter((item) => String(item || "").trim()).map((item) => humanizeReason(item) || humanizeIdentifier(item))
        : [];
    const fillBySource = Array.isArray(ledgerSummary.fill_by_source) ? ledgerSummary.fill_by_source : [];
    const fillBySide = Array.isArray(ledgerSummary.fill_by_side) ? ledgerSummary.fill_by_side : [];
    const startupReady = eodStartup.ready === false
      ? false
      : report30.startup_ready === false || report12.startup_ready === false
        ? false
        : eodStartup.ready === true || report30.startup_ready === true || report12.startup_ready === true;
    const [startupCls, startupTag] = startupReady === false
      ? ["danger", te("report_status", "blocked", t("common.status.fail"))]
      : startupReady === true
        ? ["ok", te("report_status", "ready", t("common.status.ok"))]
        : ["cancel", te("report_status", "unknown", t("common.unknown"))];
    const [recon30Cls, recon30Tag] = reportDecisionMeta(report30.final_recommendation || report30.recommendation, report30.reconciliation_status);
    const [recon12Cls, recon12Tag] = reportDecisionMeta(report12.final_recommendation || report12.recommendation, report12.reconciliation_status);
    const [eodCls, eodTag] = reportStatusMeta(eod.status || eodReconciliation.status || "unknown");
    const report30Freshness = reportFreshnessMeta(report30, 30 * 60, now);
    const report12Freshness = reportFreshnessMeta(report12, 12 * 60 * 60, now);
    const eodFreshness = reportFreshnessMeta(eod, 24 * 60 * 60, now);
    const internalDiff = Number(eodReconciliation.internal_vs_ledger_diff || 0);
    const fillCount = Number(ledgerSummary.fill_count || 0);
    metaEl.textContent = t("monitor.meta", {
      monitor30Age: freshnessAgeLabel(report30Freshness),
      monitor12Age: freshnessAgeLabel(report12Freshness),
      eodAge: eodFreshness.ageLabel,
    });

    summaryEl.innerHTML = [
      `<div class="review-chip"><span>${t("monitor.summary.startupReady")}</span><b><span class="tag ${startupCls}">${startupTag}</span></b></div>`,
      `<div class="review-chip"><span>${t("monitor.summary.monitor30")}</span><b><span class="tag ${recon30Cls}">${recon30Tag}</span></b></div>`,
      `<div class="review-chip"><span>${t("monitor.summary.monitor12")}</span><b><span class="tag ${recon12Cls}">${recon12Tag}</span></b></div>`,
      `<div class="review-chip"><span>${t("monitor.summary.eod")}</span><b><span class="tag ${eodCls}">${eodTag}</span></b></div>`,
      `<div class="review-chip"><span>${t("monitor.summary.ledgerGap")}</span><b class="${clsForValue(internalDiff)}">${fmtUsd(internalDiff)}</b></div>`,
      `<div class="review-chip"><span>${t("monitor.summary.todayFills")}</span><b>${t("monitor.summary.fills", { count: fillCount, notional: fmtUsd(ledgerSummary.fill_notional || 0, false) })}</b></div>`,
    ].join("");

    let calloutCls = "ok";
    let calloutTag = t("monitor.callout.alignedTag");
    let calloutTitle = t("monitor.callout.alignedTitle");
    let calloutBody = t("monitor.callout.alignedBody", {
      monitor30Age: freshnessAgeLabel(report30Freshness),
      monitor12Age: freshnessAgeLabel(report12Freshness),
      eodAge: eodFreshness.ageLabel,
    });
    if (startupReady === false) {
      calloutCls = "danger";
      calloutTag = t("monitor.callout.startupBlockedTag");
      calloutTitle = t("monitor.callout.startupBlockedTitle");
      calloutBody = t("monitor.callout.startupBlockedBody");
    } else if (String(eod.status || "").toLowerCase() === "fail" || String(report12.reconciliation_status || "").toLowerCase() === "fail" || String(report30.reconciliation_status || "").toLowerCase() === "fail") {
      calloutCls = "danger";
      calloutTag = t("monitor.callout.reconciliationFailedTag");
      calloutTitle = t("monitor.callout.reconciliationFailedTitle");
      calloutBody = String(issues[0] || report12.final_recommendation || report30.final_recommendation || recommendations[0] || t("monitor.callout.reconciliationFailedBodyFallback"));
    } else if (String(eod.status || "").toLowerCase() === "warn" || String(report12.reconciliation_status || "").toLowerCase() === "warn" || String(report30.reconciliation_status || "").toLowerCase() === "warn" || report12Freshness.stale || report30Freshness.stale) {
      calloutCls = "wait";
      calloutTag = t("monitor.callout.observeTag");
      calloutTitle = t("monitor.callout.observeTitle");
      calloutBody = String(
        report12Freshness.stale || report30Freshness.stale
          ? t("monitor.callout.observeBodyStale", {
            monitor30Age: freshnessAgeLabel(report30Freshness),
            monitor12Age: freshnessAgeLabel(report12Freshness),
          })
          : issues[0] || report12.final_recommendation || report30.final_recommendation || recommendations[0] || t("monitor.callout.observeBodyFallback")
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
      const [freshCls, freshTag] = freshness.stale
        ? ["danger", freshnessAgeLabel(freshness)]
        : ["wait", freshness.ageLabel];
      const rawRecommendation = String(report.recommendation || "").trim();
      const finalRecommendation = String(report.final_recommendation || "").trim();
      const recommendationText = finalRecommendation && rawRecommendation && finalRecommendation !== rawRecommendation
        ? t("monitor.window.recommendationDiff", {
          final: finalRecommendation,
          raw: rawRecommendation,
        })
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
        `<li><div class="review-main"><span>${t("monitor.window.label", { label })}</span><b><span class="tag ${sampleCls}">${sampleTag}</span> <span class="tag ${decisionCls}">${decisionTag}</span> <span class="tag ${freshCls}">${freshTag}</span></b></div><div class="review-sub">${generated > 0 ? t("monitor.window.generatedValue", { time: fmtDateTime(generated), age: historyAgeLabel(generated, now) }) : t("monitor.window.notGenerated")}</div></li>`,
        `<li><div class="review-main"><span>${t("monitor.window.samplesTitle")}</span><b>${t("monitor.window.execValue", { count: exec })}</b></div><div class="review-sub">${t("monitor.window.samplesDetail", {
          skipMax,
          cooldown: addCd,
          timeExit,
          rejectSuffix: reject > 0 ? t("monitor.window.samplesRejectSuffix", { reject }) : "",
        })}</div></li>`,
        `<li><div class="review-main"><span>${t("monitor.window.syncTitle")}</span><b><span class="tag ${reconCls}">${reconTag}</span></b></div><div class="review-sub">${t("monitor.window.syncDetail", {
          skipRatio,
          exitRatio,
          rejectSuffix: reject > 0 || ratios.reject_wallet_failures_per_exec != null ? t("monitor.window.syncRejectSuffix", { rejectRatio }) : "",
          reconcileAge: recAge > 0 ? fmtAge(recAge) : t("common.dash"),
          eventAge: eventAge > 0 ? fmtAge(eventAge) : t("common.dash"),
        })}</div></li>`,
        `<li><div class="review-main"><span>${t("monitor.window.finalAdviceTitle")}</span><b>${decisionTag}</b></div><div class="review-sub">${recommendationText || issueText || t("monitor.window.noAdvice")}</div></li>`,
      ].join("");
    };

    renderMonitorWindow(list30El, report30, "30m", 30 * 60);
    renderMonitorWindow(list12El, report12, "12h", 12 * 60 * 60);

    const startupFailures = Number(eodStartup.failure_count || 0);
    const startupWarnings = Number(eodStartup.warning_count || 0);
    const stalePending = Number(eodReconciliation.stale_pending_orders || 0);
    const latestFillTs = Number(ledgerSummary.latest_ts || 0);
    const eodRecommendationText = recommendations.length > 0 ? recommendations.join(" / ") : t("monitor.eod.noRecommendation");
    eodListEl.innerHTML = [
      `<li><div class="review-main"><span>${t("monitor.eod.statusTitle")}</span><b><span class="tag ${eodCls}">${eodTag}</span></b></div><div class="review-sub">${t("monitor.eod.statusDetail", {
        dayKey: eod.day_key || t("common.dash"),
        generated: Number(eod.generated_ts || 0) > 0 ? fmtDateTime(eod.generated_ts) : t("common.notGenerated"),
        latestFill: latestFillTs > 0 ? historyAgeLabel(latestFillTs, now || latestFillTs) : t("common.notRecorded"),
      })}</div></li>`,
      `<li><div class="review-main"><span>${t("monitor.eod.pnlTitle")}</span><b class="${clsForValue(ledgerSummary.realized_pnl || 0)}">${fmtUsd(ledgerSummary.realized_pnl || 0)}</b></div><div class="review-sub">${t("monitor.eod.pnlDetail", {
        internalDiff: fmtUsd(eodReconciliation.internal_vs_ledger_diff || 0),
        brokerGap: fmtUsd(eodReconciliation.broker_floor_gap_vs_internal || 0),
      })}</div></li>`,
      `<li><div class="review-main"><span>${t("monitor.eod.startupPendingTitle")}</span><b>${t("monitor.eod.startupPendingValue", { failures: startupFailures, warnings: startupWarnings })}</b></div><div class="review-sub">${t("monitor.eod.startupPendingDetail", {
        stalePending,
        openCount: Number(eod.state_summary && eod.state_summary.open_positions || 0),
        tracked: fmtUsd(eod.state_summary && eod.state_summary.tracked_notional_usd || 0, false),
      })}</div></li>`,
      `<li><div class="review-main"><span>${t("monitor.eod.actionsTitle")}</span><b>${recommendations.length}</b></div><div class="review-sub">${t("monitor.eod.actionsDetail", {
        issuesPrefix: issues.length > 0 ? t("monitor.eod.actionsIssuesPrefix", { issues: issues.join("; ") }) : "",
        recommendations: eodRecommendationText,
      })}</div></li>`,
    ].join("");

    const breakdownCards = [];
    fillBySource.slice(0, 4).forEach((bucket) => {
      breakdownCards.push(`<div class="component-card">
        <span>${t("monitor.breakdown.sourceCard", { source: String(bucket.source_label || humanizeIdentifier(bucket.source || t("common.unknown"))) })}</span>
        <b>${t("monitor.breakdown.fills", { count: Number(bucket.fill_count || 0) })}</b>
        <div class="tiny-list">
          <span><i>${t("monitor.breakdown.amount")}</i><strong>${fmtUsd(bucket.notional || 0, false)}</strong></span>
          <span><i>${t("monitor.breakdown.realized")}</i><strong class="${clsForValue(bucket.realized_pnl || 0)}">${fmtUsd(bucket.realized_pnl || 0)}</strong></span>
        </div>
      </div>`);
    });
    fillBySide.slice(0, 2).forEach((bucket) => {
      breakdownCards.push(`<div class="component-card">
        <span>${t("monitor.breakdown.sideCard", { side: String(bucket.side_label || sideLabel(bucket.side || "UNKNOWN")) })}</span>
        <b>${t("monitor.breakdown.fills", { count: Number(bucket.fill_count || 0) })}</b>
        <div class="tiny-list">
          <span><i>${t("monitor.breakdown.amount")}</i><strong>${fmtUsd(bucket.notional || 0, false)}</strong></span>
          <span><i>${t("monitor.breakdown.realized")}</i><strong class="${clsForValue(bucket.realized_pnl || 0)}">${fmtUsd(bucket.realized_pnl || 0)}</strong></span>
        </div>
      </div>`);
    });
    breakdownMetaEl.textContent = t("monitor.breakdown.metaValue", {
      sources: fillBySource.length,
      sides: fillBySide.length,
    });
    breakdownEl.innerHTML = breakdownCards.length > 0
      ? breakdownCards.join("")
      : `<div class="component-card"><span>${t("monitor.breakdown.emptyLabel")}</span><b>${t("monitor.breakdown.emptyValue")}</b></div>`;
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
    const eodIssues = Array.isArray(eod.issue_labels) && eod.issue_labels.length > 0
      ? eod.issue_labels
      : Array.isArray(eod.issues)
        ? eod.issues.map((item) => humanizeReason(item) || humanizeIdentifier(item))
        : [];
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

    metaEl.textContent = t("diagnostics.panel.metaValue", {
      startupAge: Number(state.ts || 0) > 0 ? historyAgeLabel(state.ts, now) : t("common.notRecorded"),
      monitor30Age: freshnessAgeLabel(report30Freshness),
      monitor12Age: freshnessAgeLabel(report12Freshness),
      eodAge: eodFreshness.ageLabel,
    });

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
              <span>${startupCheckNameLabel(row && row.name || "")}</span>
              <b><span class="tag ${cls}">${tag}</span></b>
            </div>
            <div class="review-sub">${String(row && row.message || t("diagnostics.startup.messageFallback"))}${detailText ? ` · ${detailText}` : ""}</div>
          </li>`;
        }).join("")
      : `<li><div class="review-main"><span>${t("diagnostics.startup.empty")}</span><b>--</b></div></li>`;

    const facts = [
      {
        label: t("diagnostics.facts.internalLedgerLabel"),
        value: fmtUsd(reconciliation.internal_vs_ledger_diff || 0),
        sub: t("diagnostics.facts.internalLedgerSub", {
          brokerGap: fmtUsd(reconciliation.broker_floor_gap_vs_internal || 0),
        }),
        cls: clsForValue(reconciliation.internal_vs_ledger_diff || 0),
      },
      {
        label: t("diagnostics.facts.pendingStaleLabel"),
        value: `${Number(reconciliation.pending_orders || 0)} / ${Number(reconciliation.stale_pending_orders || 0)}`,
        sub: t("diagnostics.facts.pendingStaleSub", {
          entry: Number(reconciliation.pending_entry_orders || 0),
          exit: Number(reconciliation.pending_exit_orders || 0),
        }),
        cls: Number(reconciliation.stale_pending_orders || 0) > 0 ? "value-negative" : "value-neutral",
      },
      {
        label: t("diagnostics.facts.snapshotAgeLabel"),
        value: fmtAge(reconciliation.account_snapshot_age_seconds || 0),
        sub: t("diagnostics.facts.snapshotAgeSub", {
          reconcileAge: fmtAge(reconciliation.broker_reconcile_age_seconds || 0),
          eventAge: fmtAge(reconciliation.broker_event_sync_age_seconds || 0),
        }),
        cls: Number(reconciliation.account_snapshot_age_seconds || 0) > 1800 ? "value-negative" : "value-neutral",
      },
      {
        label: t("diagnostics.facts.fillsTodayLabel"),
        value: `${Number(reconciliation.fill_count_today || 0)}`,
        sub: t("diagnostics.facts.fillsTodaySub", {
          notional: fmtUsd(reconciliation.fill_notional_today || 0, false),
          syncCount: Number(reconciliation.account_sync_count_today || 0),
        }),
        cls: "value-neutral",
      },
      {
        label: t("diagnostics.facts.startupChecksTodayLabel"),
        value: `${Number(reconciliation.startup_checks_count_today || 0)}`,
        sub: t("diagnostics.facts.startupChecksTodaySub", {
          lastFill: Number(reconciliation.last_fill_ts || 0) > 0 ? historyAgeLabel(reconciliation.last_fill_ts, now) : t("common.notRecorded"),
        }),
        cls: "value-neutral",
      },
      {
        label: t("diagnostics.facts.openTrackedLabel"),
        value: `${Number(reconciliation.open_positions || 0)} / ${fmtUsd(reconciliation.tracked_notional_usd || 0, false)}`,
        sub: t("diagnostics.facts.openTrackedSub", {
          status: te("report_status", String(reconciliation.status || "unknown").toLowerCase(), String(reconciliation.status || "unknown").toUpperCase()),
        }),
        cls: "value-neutral",
      },
      {
        label: t("diagnostics.facts.operatorCleanupLabel"),
        value: operatorMeta.label,
        sub: operatorRequestTs > 0
          ? t("diagnostics.facts.operatorCleanupSub", {
            processed: operatorActionProcessedTs > 0
              ? t("diagnostics.facts.operatorCleanupProcessed", { time: fmtDateTime(operatorActionProcessedTs) })
              : t("diagnostics.facts.operatorCleanupQueued"),
            requested: fmtDateTime(operatorRequestTs),
            remainingSuffix: operatorActionStatus.remaining_pending_orders != null
              ? t("diagnostics.facts.operatorCleanupRemaining", { count: Number(operatorActionStatus.remaining_pending_orders || 0) })
              : "",
          })
          : t("diagnostics.facts.operatorCleanupWaiting"),
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

    const stateIssues = Array.isArray(reconciliation.issues) ? reconciliation.issues.map((item) => issueDisplayText(item)) : [];
    stateIssues.slice(0, 4).forEach((item) => pushIssue(t("diagnostics.issues.stateReconciliationTitle"), String(item), String(reconciliation.status || "wait")));
    if (report30.final_recommendation || report30Freshness.stale) {
      pushIssue(t("diagnostics.issues.monitor30Title"), monitorWindowDisplaySummary(report30, 30 * 60, now), report30Freshness.stale ? "warn" : (recommendationKind(report30.final_recommendation) === "ready" ? "ok" : recommendationKind(report30.final_recommendation)));
    }
    if (report12.final_recommendation || report12Freshness.stale) {
      pushIssue(t("diagnostics.issues.monitor12Title"), monitorWindowDisplaySummary(report12, 12 * 60 * 60, now), report12Freshness.stale ? "warn" : (recommendationKind(report12.final_recommendation) === "ready" ? "ok" : recommendationKind(report12.final_recommendation)));
    }
    eodIssues.slice(0, 3).forEach((item) => pushIssue(t("diagnostics.issues.eodIssueTitle"), String(item), String(eod.status || "wait")));
    eodRecommendations.slice(0, 3).forEach((item) => pushIssue(t("diagnostics.issues.eodRecommendationTitle"), String(item), String(eod.status || "wait")));
    if (operatorRequestTs > 0) {
      if (operatorRequestTs > operatorActionProcessedTs) {
        pushIssue(t("diagnostics.issues.operatorActionTitle"), t("diagnostics.issues.operatorRequestedDetail"), "wait");
      } else if (String(operatorActionStatus.message || "").trim()) {
        const operatorIssueStatus = String(operatorActionStatus.status || "wait");
        pushIssue(
          t("diagnostics.issues.operatorActionTitle"),
          String(operatorActionStatus.message || ""),
          operatorIssueStatus === "cleared" || operatorIssueStatus === "noop" ? "ok" : "wait"
        );
      }
    }
    if (issueRows.length === 0) {
      pushIssue(t("diagnostics.issues.currentDiagnosisTitle"), t("diagnostics.issues.currentDiagnosisDetail"), "ok");
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
    pendingMetaEl.textContent = stalePending > 0
      ? t("diagnostics.pending.metaValueWithStale", {
        orders: pendingOrders.length,
        stale: stalePending,
      })
      : t("diagnostics.pending.metaValue", { orders: pendingOrders.length });
    replaceRows(
      pendingBodyEl,
      pendingOrders.slice(0, 12).map((row) => {
        const [statusCls, statusTag] = pendingOrderStatusMeta(row.broker_status || "live");
        const [flowCls, flowTag] = actionTagMeta(
          row.flow === "exit" ? "exit" : "entry",
          row.flow === "exit"
            ? t("diagnostics.pending.flowExit")
            : t("diagnostics.pending.flowEntry")
        );
        const requestedText = row.requested_notional > 0
          ? `${fmtUsd(row.requested_notional || 0, false)} @ ${Number(row.requested_price || 0).toFixed(4)}`
          : t("common.dash");
        const matchedText = row.matched_notional_hint > 0
          ? `${fmtUsd(row.matched_notional_hint || 0, false)} @ ${Number(row.matched_price_hint || 0).toFixed(4)}`
          : t("common.dash");
        const reasonText = String(row.reason || row.message || t("common.dash"));
        const rowClass = selectedDiagnosticFocusKind === "pending" && selectedDiagnosticFocusKey === row._diagKey
          ? "click-row active-row"
          : "click-row";
        return `<tr class="${rowClass}" data-diagnostic-kind="pending" data-diagnostic-key="${attrToken(row._diagKey)}">
          <td class="wrap">
            <div class="cell-stack">
              <span class="cell-main">${Number(row.ts || 0) > 0 ? hhmm(row.ts) : t("common.dashTime")}</span>
              <span class="cell-sub">${Number(row.ts || 0) > 0 ? historyAgeLabel(row.ts, now) : t("common.notRecorded")}</span>
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
      `<tr><td colspan="5">${t("diagnostics.pending.empty")}</td></tr>`
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
        `<li><span>${t("diagnostics.focus.checkLabel")}</span><b>${startupCheckNameLabel(selectedStartup.name || "")}</b></li>`,
        `<li><span>${t("diagnostics.focus.statusLabel")}</span><b>${tag}</b></li>`,
        `<li><span>${t("diagnostics.focus.messageLabel")}</span><b>${String(selectedStartup.message || t("diagnostics.startup.messageFallback"))}</b></li>`,
      ];
      Object.entries(details).forEach(([key, value]) => {
        rows.push(`<li><span>${String(key)}</span><b>${String(value)}</b></li>`);
      });
      focusMetaEl.textContent = t("diagnostics.focus.startupMeta", {
        name: startupCheckNameLabel(selectedStartup.name || ""),
      });
      focusHeadEl.innerHTML = `<span class="tag ${cls}">${tag}</span><span class="mono">${startupCheckNameLabel(selectedStartup.name || "")}</span>`;
      focusSummaryEl.textContent = String(selectedStartup.message || t("diagnostics.focus.startupSummaryEmpty"));
      focusListEl.innerHTML = rows.join("");
      return;
    }

    if (selectedPending) {
      const [statusCls, statusTag] = pendingOrderStatusMeta(selectedPending.broker_status || "live");
      const [flowCls, flowTag] = actionTagMeta(
        selectedPending.flow === "exit" ? "exit" : "entry",
        selectedPending.flow === "exit"
          ? t("diagnostics.focus.pendingExitTag")
          : t("diagnostics.focus.pendingEntryTag")
      );
      const focusRows = [
        `<li><span>${t("diagnostics.focus.labels.marketDirection")}</span><b>${String(selectedPending.title || selectedPending.market_slug || selectedPending.token_id || "-")} · ${String(selectedPending.outcome || selectedPending.side || "-")}</b></li>`,
        `<li><span>${t("diagnostics.focus.labels.orderTrace")}</span><b>${String(selectedPending.order_id || "--")} · ${String(selectedPending.trace_id || "--")}</b></li>`,
        `<li><span>${t("diagnostics.focus.labels.requestedNotional")}</span><b>${fmtUsd(selectedPending.requested_notional || 0, false)} @ ${Number(selectedPending.requested_price || 0).toFixed(4)}</b></li>`,
        `<li><span>${t("diagnostics.focus.labels.matchedHint")}</span><b>${selectedPending.matched_notional_hint > 0 ? `${fmtUsd(selectedPending.matched_notional_hint || 0, false)} @ ${Number(selectedPending.matched_price_hint || 0).toFixed(4)}` : t("diagnostics.focus.values.matchedHintEmpty")}</b></li>`,
        `<li><span>${t("diagnostics.focus.labels.sourceWallet")}</span><b>${selectedPending.wallet ? `${sourceWalletLabel(selectedPending.wallet)}${selectedPending.wallet_tier ? ` · ${walletTierLabel(selectedPending.wallet_tier)}` : ""}${Number(selectedPending.wallet_score || 0) > 0 ? ` · ${Number(selectedPending.wallet_score || 0).toFixed(1)}` : ""}` : t("diagnostics.focus.values.sourceWalletEmpty")}</b></li>`,
        `<li><span>${t("diagnostics.focus.labels.entryWallet")}</span><b>${selectedPending.entry_wallet ? `${sourceWalletLabel(selectedPending.entry_wallet)}${selectedPending.entry_wallet_tier ? ` · ${walletTierLabel(selectedPending.entry_wallet_tier)}` : ""}${Number(selectedPending.entry_wallet_score || 0) > 0 ? ` · ${Number(selectedPending.entry_wallet_score || 0).toFixed(1)}` : ""}` : t("diagnostics.focus.values.entryWalletEmpty")}</b></li>`,
        `<li><span>${t("diagnostics.focus.labels.cycleSignal")}</span><b>${String(selectedPending.cycle_id || "--")} · ${String(selectedPending.signal_id || "--")}</b></li>`,
        `<li><span>${t("diagnostics.focus.labels.timeHeartbeat")}</span><b>${Number(selectedPending.ts || 0) > 0 ? t("diagnostics.focus.values.timeHeartbeat", {
          time: fmtDateTime(selectedPending.ts),
          age: historyAgeLabel(selectedPending.ts, now),
          heartbeatSuffix: Number(selectedPending.last_heartbeat_ts || 0) > 0
            ? t("diagnostics.focus.values.heartbeatSuffix", { age: historyAgeLabel(selectedPending.last_heartbeat_ts, now) })
            : "",
        }) : t("common.notRecorded")}</b></li>`,
      ];
      if (selectedPending.topic_label) {
        focusRows.push(`<li><span>${t("diagnostics.focus.labels.topic")}</span><b>${String(selectedPending.topic_label)}</b></li>`);
      }
      if (selectedPending.condition_id || selectedPending.token_id) {
        focusRows.push(`<li><span>${t("diagnostics.focus.labels.conditionToken")}</span><b>${String(selectedPending.condition_id || "--")} · ${String(selectedPending.token_id || "--")}</b></li>`);
      }
      focusMetaEl.textContent = t("diagnostics.focus.pendingMeta", {
        label: String(selectedPending.order_id || selectedPending.title || t("common.entity.order")),
      });
      focusHeadEl.innerHTML =
        `<span class="tag ${flowCls}">${flowTag}</span>` +
        `<span class="tag ${statusCls}">${statusTag}</span>` +
        `<span class="mono">${String(selectedPending.broker_status || "pending")}</span>`;
      focusSummaryEl.textContent = String(selectedPending.reason || selectedPending.message || t("diagnostics.focus.pendingSummaryEmpty"));
      focusListEl.innerHTML = focusRows.join("");
      return;
    }

    focusMetaEl.textContent = t("diagnostics.focus.notSelected");
    focusHeadEl.innerHTML = `<span class="tag danger">${t("diagnostics.focus.waitingTag")}</span><span class="mono">${t("diagnostics.focus.waitingHint")}</span>`;
    focusSummaryEl.textContent = t("diagnostics.focus.waitingSummary");
    focusListEl.innerHTML = `<li><span>${t("diagnostics.focus.list.status")}</span><b>${t("common.waitingData")}</b></li>`;
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
        button.textContent = ok
          ? t("common.actionState.copied")
          : t("common.actionState.copyFailed");
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
          button.textContent = t("common.actionState.refreshed");
          await refresh();
        } catch (_err) {
          button.textContent = t("common.actionState.refreshFailed");
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
      titleEl.textContent = t("wallet.detail.empty.title");
      headEl.innerHTML = `<span class="tag danger">${escapeHtml(t("common.waiting"))}</span><span class="mono">${escapeHtml(t("wallet.detail.empty.subtitle"))}</span>`;
      summaryEl.textContent = t("wallet.detail.empty.summary");
      listEl.innerHTML = `<li><span>${escapeHtml(t("wallet.detail.list.status"))}</span><b>${escapeHtml(t("common.waitingData"))}</b></li>`;
      componentEl.innerHTML = `<div class="component-card"><span>${escapeHtml(t("walletProfiles.detail.scoreBreakdown"))}</span><b>${escapeHtml(t("common.waitingData"))}</b></div>`;
      topicMetaEl.textContent = t("walletProfiles.detail.topicCount");
      topicGridEl.innerHTML = `<div class="topic-card"><span>${escapeHtml(t("walletProfiles.detail.topicCard"))}</span><b>${escapeHtml(t("common.waitingData"))}</b></div>`;
      historyMetaEl.textContent = t("walletProfiles.detail.sampleCount");
      historyBodyEl.innerHTML = `<tr><td colspan="5">${escapeHtml(t("common.waitingData"))}</td></tr>`;
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
      `<span class="tag ${tierTagClass(tier)}">${walletTierLabel(tier)}</span>` +
      `<span class="tag ${history.cls}">${history.label}</span>` +
      `<span class="mono">${tradingEnabled ? t("wallet.detail.mode.tradeEnabled") : t("wallet.detail.mode.observeOnly")}</span>`;
    summaryEl.textContent = scoreSummary || t("wallet.detail.summaryEmpty");

    listEl.innerHTML = [
      `<li><span>${escapeHtml(t("wallet.detail.list.status"))}</span><b>${t("wallet.detail.values.status", { score: Number(wallet.score || 0).toFixed(1), mode: tradingEnabled ? t("wallet.detail.mode.tradeEnabled") : t("wallet.detail.mode.observeOnly") })}</b></li>`,
      `<li><span>${escapeHtml(t("wallet.detail.list.positionProfile"))}</span><b>${t("wallet.detail.values.positionProfile", { positions: Number(wallet.positions || 0), markets: Number(wallet.unique_markets || 0), notional: Number(wallet.notional || 0).toFixed(0) })}</b></li>`,
      `<li><span>${escapeHtml(t("wallet.detail.list.history"))}</span><b>${t("wallet.detail.values.history", { closed: Number(wallet.closed_positions || 0), resolved: Number(wallet.resolved_markets || 0), age: historyAgeLabel(wallet.history_refresh_ts, now) })}</b></li>`,
      `<li><span>${escapeHtml(t("wallet.detail.list.performance"))}</span><b class="${clsForValue(wallet.roi)}">${t("wallet.detail.values.performance", { roi: fmtSignedRatioPct(wallet.roi, 1), winRate: fmtRatioPct(wallet.win_rate, 1) })}</b></li>`,
      `<li><span>${escapeHtml(t("wallet.detail.list.resolutionPf"))}</span><b>${t("wallet.detail.values.resolutionPf", { resolvedWinRate: Number(wallet.resolved_markets || 0) > 0 ? fmtRatioPct(wallet.resolved_win_rate, 1) : "--", profitFactor: Number(wallet.profit_factor || 0).toFixed(2) })}</b></li>`,
      `<li><span>${escapeHtml(t("wallet.detail.list.realizedPnl"))}</span><b class="${clsForValue(realizedPnl)}">${t("wallet.detail.values.realizedPnl", { realizedPnl: fmtUsd(realizedPnl), totalBought: fmtUsd(totalBought, false) })}</b></li>`,
      `<li><span>${escapeHtml(t("wallet.detail.list.activity"))}</span><b>${wallet.activity_known ? t("wallet.detail.values.activity", { count: Number(wallet.recent_activity_events || 0) }) : t("common.unknown")}</b></li>`,
      `<li><span>${escapeHtml(t("wallet.detail.list.poolPriority"))}</span><b>${t("wallet.detail.values.poolPriority", { rank: Number(wallet.discovery_priority_rank || 0), score: Number(wallet.discovery_priority_score || 0).toFixed(2), topicSuffix: wallet.discovery_best_topic ? ` · ${wallet.discovery_best_topic}` : "" })}</b></li>`,
      `<li><span>${escapeHtml(t("wallet.detail.list.winBreakdown"))}</span><b>${t("wallet.detail.values.winBreakdown", { wins, closed: Number(wallet.closed_positions || 0), resolvedWins, resolvedMarkets: Number(wallet.resolved_markets || 0) })}</b></li>`,
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
      : `<div class="component-card"><span>${escapeHtml(t("walletProfiles.detail.scoreBreakdown"))}</span><b>${escapeHtml(t("common.none"))}</b></div>`;

    topicMetaEl.textContent = t("wallet.detail.topicCount", { count: topicProfiles.length });
    topicGridEl.innerHTML = topicProfiles.length > 0
      ? topicProfiles
          .map((topic) => {
            const resolvedText = Number(topic.resolved_markets || 0) > 0
              ? t("wallet.detail.values.topicResolved", { resolvedWinRate: fmtRatioPct(topic.resolved_win_rate, 0) })
              : t("wallet.detail.history.resolvedEmpty");
            return `<div class="topic-card">
              <span>${String(topic.label || topic.key || t("common.unknown"))} · ${(Number(topic.sample_share || 0) * 100).toFixed(0)}%</span>
              <b class="${topicTone(topic)}">${fmtSignedRatioPct(topic.roi, 1)} / ${fmtRatioPct(topic.win_rate, 0)}</b>
              <span>${t("wallet.detail.history.sampleCount", { count: Number(topic.sample_count || 0) })} · ${resolvedText}</span>
            </div>`;
          })
          .join("")
      : `<div class="topic-card"><span>${escapeHtml(t("walletProfiles.detail.topicCard"))}</span><b>${escapeHtml(t("common.none"))}</b></div>`;

    historyMetaEl.textContent = t("wallet.detail.history.sampleCount", { count: recentClosedMarkets.length });
    historyBodyEl.innerHTML = recentClosedMarkets.length > 0
      ? recentClosedMarkets
          .map((sample) => {
            const [verdictCls, verdictText] = sampleVerdict(sample);
            const winnerText = sample.winner_outcome
              ? t("wallet.detail.values.sampleResolvedWinner", { winner: sample.winner_outcome })
              : "";
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
                  <span class="cell-sub">${sample.resolved ? t("wallet.detail.values.sampleResolved", { winnerSuffix: winnerText }) : t("wallet.detail.history.unresolved")}</span>
                </div>
              </td>
              <td class="${clsForValue(sample.roi)}">${fmtSignedRatioPct(sample.roi, 1)}</td>
              <td class="${clsForValue(sample.realized_pnl)}">${fmtUsd(sample.realized_pnl)}</td>
            </tr>`;
          })
          .join("")
      : `<tr><td colspan="5">${escapeHtml(t("wallet.detail.history.empty"))}</td></tr>`;
  }

  function updateModeBadge(config) {
    const pill = $("mode-pill");
    if (!pill) return;
    const mode = String(config.execution_mode || (config.dry_run ? "paper" : "live")).toLowerCase();
    const label = te("execution_mode", mode === "live" ? "live" : "paper", mode === "live" ? t("app.modeBadge.live") : t("app.modeBadge.paper"));
    pill.textContent = label;
    pill.classList.toggle("live", mode === "live");
    pill.classList.toggle("paper", mode !== "live");
  }

  function updateStrategyParams(config) {
    if ($("param-wallet-pool")) {
      $("param-wallet-pool").textContent = t("strategy.params.value.walletPool", {
        count: Number(config.wallet_pool_size || 0),
      }, `wallet_pool=${Number(config.wallet_pool_size || 0)}`);
    }
    if ($("param-min-increase")) {
      $("param-min-increase").textContent = t("strategy.params.value.minIncrease", {
        amount: Number(config.min_wallet_increase_usd || 0).toFixed(0),
      }, `${Number(config.min_wallet_increase_usd || 0).toFixed(0)} USD`);
    }
    if ($("param-max-signals")) $("param-max-signals").textContent = String(config.max_signals_per_cycle || 0);
    if ($("param-min-wallet-score")) $("param-min-wallet-score").textContent = Number(config.min_wallet_score || 0).toFixed(1);
    if ($("param-score-multipliers")) {
      $("param-score-multipliers").textContent = t("strategy.params.value.scoreMultipliers", {
        watch: Number(config.wallet_score_watch_multiplier || 0).toFixed(2),
        trade: Number(config.wallet_score_trade_multiplier || 0).toFixed(2),
        core: Number(config.wallet_score_core_multiplier || 0).toFixed(2),
      }, `W ${Number(config.wallet_score_watch_multiplier || 0).toFixed(2)} / T ${Number(config.wallet_score_trade_multiplier || 0).toFixed(2)} / C ${Number(config.wallet_score_core_multiplier || 0).toFixed(2)}`);
    }
    if ($("param-history-window")) {
      $("param-history-window").textContent = t("strategy.params.value.historyWindow", {
        window: fmtAge(Number(config.wallet_history_refresh_seconds || 0)),
        minClosed: Number(config.history_min_closed_positions || 0),
        strongClosed: Number(config.history_strong_closed_positions || 0),
        strongResolved: Number(config.history_strong_resolved_markets || 0),
      }, `${fmtAge(Number(config.wallet_history_refresh_seconds || 0))} / min ${Number(config.history_min_closed_positions || 0)} / strong ${Number(config.history_strong_closed_positions || 0)}c ${Number(config.history_strong_resolved_markets || 0)}r`);
    }
    if ($("param-topic-bias")) {
      if (!config.topic_bias_enabled) {
        $("param-topic-bias").textContent = t("strategy.params.value.disabled");
      } else {
        $("param-topic-bias").textContent = t("strategy.params.value.topicBias", {
          minSamples: Number(config.topic_min_samples || 0),
          boost: Number(config.topic_boost_multiplier || 0).toFixed(2),
          penalty: Number(config.topic_penalty_multiplier || 0).toFixed(2),
        }, `min ${Number(config.topic_min_samples || 0)} / +${Number(config.topic_boost_multiplier || 0).toFixed(2)} / -${Number(config.topic_penalty_multiplier || 0).toFixed(2)}`);
      }
    }
    if ($("param-discovery-bias")) {
      if (!config.wallet_discovery_quality_bias_enabled) {
        $("param-discovery-bias").textContent = t("strategy.params.value.disabled");
      } else {
        $("param-discovery-bias").textContent = t("strategy.params.value.discoveryBias", {
          topN: Number(config.wallet_discovery_quality_top_n || 0),
          historyBonus: Number(config.wallet_discovery_history_bonus || 0).toFixed(2),
          topicBonus: Number(config.wallet_discovery_topic_bonus || 0).toFixed(2),
        }, `top ${Number(config.wallet_discovery_quality_top_n || 0)} / hist +${Number(config.wallet_discovery_history_bonus || 0).toFixed(2)} / topic +${Number(config.wallet_discovery_topic_bonus || 0).toFixed(2)}`);
      }
    }
    if ($("param-wallet-exit")) {
      if (!config.wallet_exit_follow_enabled) {
        $("param-wallet-exit").textContent = t("strategy.params.value.disabled");
      } else if (!config.resonance_exit_enabled) {
        $("param-wallet-exit").textContent = t("strategy.params.value.walletExitSingle", {
          amount: Number(config.min_wallet_decrease_usd || 0).toFixed(0),
        }, `single / min ${Number(config.min_wallet_decrease_usd || 0).toFixed(0)} USD`);
      } else {
        $("param-wallet-exit").textContent = t("strategy.params.value.walletExitResonance", {
          amount: Number(config.min_wallet_decrease_usd || 0).toFixed(0),
          wallets: Number(config.resonance_min_wallets || 0),
          trim: fmtPct(Number(config.resonance_trim_fraction || 0) * 100, 0),
          coreExit: fmtPct(Number(config.resonance_core_exit_fraction || 0) * 100, 0),
        }, `single ${Number(config.min_wallet_decrease_usd || 0).toFixed(0)} / res ${Number(config.resonance_min_wallets || 0)} wallets / ${fmtPct(Number(config.resonance_trim_fraction || 0) * 100, 0)} -> ${fmtPct(Number(config.resonance_core_exit_fraction || 0) * 100, 0)}`);
      }
    }
    if ($("param-risk-trade")) $("param-risk-trade").textContent = fmtPct(Number(config.risk_per_trade_pct || 0) * 100, 2);
    if ($("param-risk-day")) $("param-risk-day").textContent = fmtPct(Number(config.daily_max_loss_pct || 0) * 100, 2);
    if ($("param-price-band")) $("param-price-band").textContent = `${Number(config.min_price || 0).toFixed(2)} ~ ${Number(config.max_price || 0).toFixed(2)}`;
    if ($("param-add-cooldown")) {
      $("param-add-cooldown").textContent = t("strategy.params.value.addCooldown", {
        seconds: Number(config.token_add_cooldown_seconds || 0),
      }, `${Number(config.token_add_cooldown_seconds || 0)}s`);
    }
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
    if ($("chip-pending")) $("chip-pending").textContent = t("orders.status.pendingCount", { count: pending });
    if ($("chip-filled")) $("chip-filled").textContent = t("orders.status.filledCount", { count: filled });
    if ($("chip-canceled")) $("chip-canceled").textContent = t("orders.status.canceledCount", { count: canceled });
    if ($("chip-rejected")) $("chip-rejected").textContent = t("orders.status.rejectedCount", { count: rejected });
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
      $("meter-dd-text").textContent = t("risk.meters.drawdownValue", {
        used: fmtPct(used, 1),
        limit: fmtPct(Number(config.daily_max_loss_pct || 0) * 100, 1),
      });
    }

    if ($("meter-exp")) $("meter-exp").value = Math.max(0, Math.min(100, slotUtil));
    if ($("meter-exp-text")) $("meter-exp-text").textContent = t("risk.meters.exposureValue", { open: openPos, max: maxOpen });

    if ($("meter-cooldown")) $("meter-cooldown").value = Math.max(0, Math.min(100, cooldownRate));
    if ($("meter-cooldown-text")) $("meter-cooldown-text").textContent = t("risk.meters.cooldownValue", { skips: cooldownSkips, orders: orders.length });

    if ($("meter-util")) $("meter-util").value = Math.max(0, Math.min(100, freshnessPct));
    if ($("meter-util-text")) $("meter-util-text").textContent = t("risk.meters.stalenessValue", { stateAge: stateAgeSec, poll: pollSec });
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
      pauseBtn.textContent = controlState.pause_opening ? t("control.resumeOpening") : t("control.pauseOpening");
    }
    if (reduceBtn) {
      reduceBtn.classList.toggle("active", controlState.reduce_only);
      reduceBtn.textContent = controlState.reduce_only ? t("control.clearReduceOnly") : t("control.reduceOnly");
    }
    if (emergencyBtn) {
      emergencyBtn.classList.toggle("active", controlState.emergency_stop);
      emergencyBtn.textContent = controlState.emergency_stop ? t("control.clearEmergencyStop") : t("control.emergencyStop");
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
        decisionConsoleNotice = { cls: "wait", text: t("candidate.modeSwitch.pending", { mode: modeLabel(mode) }) };
        renderDecisionConsole(lastDecisionCandidates, lastDecisionMode, lastDecisionApiState);
        setButtonBusy(button, true);
        try {
          const next = await pushMode(mode);
          lastDecisionMode = {
            ...lastDecisionMode,
            ...(next && typeof next === "object" ? next : {}),
          };
          decisionConsoleNotice = { cls: "ok", text: t("candidate.modeSwitch.success", { mode: modeLabel(mode) }) };
          renderDecisionConsole(lastDecisionCandidates, lastDecisionMode, lastDecisionApiState);
          await refresh();
        } catch (_err) {
          decisionConsoleNotice = { cls: "danger", text: t("candidate.modeSwitch.fail", { mode: modeLabel(mode) }) };
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
          message: t("candidate.request.pending", { action: candidateActionText(action, candidateSide) }),
        };
        renderDecisionConsole(lastDecisionCandidates, lastDecisionMode, lastDecisionApiState);
        setButtonBusy(button, true);
        try {
          await pushCandidateAction(candidateId, action, "");
          candidateRequestState[candidateId] = {
            kind: "success",
            message: t("candidate.request.success", { action: candidateActionText(action, candidateSide) }),
          };
          renderDecisionConsole(lastDecisionCandidates, lastDecisionMode, lastDecisionApiState);
          await refresh();
        } catch (_err) {
          candidateRequestState[candidateId] = {
            kind: "error",
            message: t("candidate.request.fail", { action: candidateActionText(action, candidateSide) }),
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
        walletProfileRequestState[wallet] = { kind: "success", message: t("walletProfiles.rowState.noChange") };
        renderWalletProfilesPanel(lastWalletProfiles, lastWalletProfilesApiState);
        return;
      }
      walletProfileRequestState[wallet] = { kind: "pending", message: t("walletProfiles.rowState.saving") };
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
        walletProfileRequestState[wallet] = { kind: "success", message: t("walletProfiles.rowState.saved") };
        renderWalletProfilesPanel(lastWalletProfiles, lastWalletProfilesApiState);
        await refresh();
      } catch (_err) {
        walletProfileRequestState[wallet] = { kind: "error", message: t("walletProfiles.rowState.error") };
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
        journalComposerNotice = { cls: "danger", text: t("journal.composer.empty") };
        renderJournalPanel(lastJournalSummary, lastJournalApiState);
        return;
      }
      journalComposerNotice = { cls: "wait", text: t("journal.composer.saving") };
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
        journalComposerNotice = { cls: "ok", text: t("journal.composer.saved") };
        renderJournalPanel(lastJournalSummary, lastJournalApiState);
        await refresh();
      } catch (_err) {
        journalComposerNotice = { cls: "danger", text: t("journal.composer.saveFailed") };
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
      const [data, monitor30m, monitor12h, reconciliationEod, candidateResult, modeResult, journalResult, walletProfilesResult, blockbeatsResult] = await Promise.all([
        fetchJson("/api/state", null),
        fetchJson("/api/monitor/30m", EMPTY_MONITOR_REPORT("monitor_30m")),
        fetchJson("/api/monitor/12h", EMPTY_MONITOR_REPORT("monitor_12h")),
        fetchJson("/api/reconciliation/eod", EMPTY_RECONCILIATION_EOD_REPORT),
        fetchJsonState("/api/candidates", EMPTY_CANDIDATES),
        fetchJsonState("/api/mode", EMPTY_DECISION_MODE),
        fetchJsonState("/api/journal", EMPTY_JOURNAL),
        fetchJsonState("/api/wallet-profiles", EMPTY_WALLET_PROFILES),
        fetchJsonState("/api/blockbeats", EMPTY_BLOCKBEATS),
      ]);
      const candidateData = candidateResult && candidateResult.data ? candidateResult.data : EMPTY_CANDIDATES;
      const modeData = modeResult && modeResult.data ? modeResult.data : EMPTY_DECISION_MODE;
      const journalData = journalResult && journalResult.data ? journalResult.data : EMPTY_JOURNAL;
      const walletProfilesData = walletProfilesResult && walletProfilesResult.data ? walletProfilesResult.data : EMPTY_WALLET_PROFILES;
      const blockbeatsData = blockbeatsResult && blockbeatsResult.data ? blockbeatsResult.data : EMPTY_BLOCKBEATS;
      const stateData = data || {};
      const now = Number(stateData.ts || 0);
      const renderNow = now || Math.floor(Date.now() / 1000);
      lastSignalReview = stateData.signal_review || {};
      lastSignalReviewNow = now;
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
      renderBlockbeatsPanel(blockbeatsData, {
        ok: !!(blockbeatsResult && blockbeatsResult.ok),
        error: String(blockbeatsResult && blockbeatsResult.error || ""),
      }, renderNow);
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
      const accountSnapshotLabel = accountSnapshotTs > 0
        ? `${hhmm(accountSnapshotTs)} · ${historyAgeLabel(accountSnapshotTs, now)}`
        : t("budget.overview.accountSyncWaiting");
      const opsGate = computeOpsGate(data, monitor30m, monitor12h, reconciliationEod, now);
      renderControlState(control);
      updateModeBadge(config);
      renderOpsGate(opsGate);

      if ($("status-line")) {
        $("status-line").textContent = t("app.statusLine", {
          time: hhmm(now),
          pollSeconds: pollSec,
          refreshSeconds: FRONTEND_REFRESH_SECONDS,
        });
      }

      if ($("guard-line")) {
        const basePerTrade = Number(summary.base_per_trade_notional || summary.per_trade_notional || 0);
        const maxPerTrade = Number(summary.theoretical_max_order_notional || 0);
        $("guard-line").textContent = t("app.guardLine", {
          mode: te("execution_mode", mode, mode.toUpperCase()),
          broker: brokerName,
          basePerTrade: basePerTrade.toFixed(2),
          maxPerTrade: maxPerTrade.toFixed(2),
        });
      }

      if ($("budget-meta")) {
        $("budget-meta").textContent = t("budget.overview.metaValue", {
          equity: accountEquity.toFixed(2),
          cash: cashBalance.toFixed(2),
          tracked: trackedNotional.toFixed(2),
          available: availableNotional.toFixed(2),
        });
      }
      if ($("budget-fill")) {
        $("budget-fill").style.width = `${Math.max(0, Math.min(100, slotUtil)).toFixed(1)}%`;
      }

      if ($("kpi-mode")) $("kpi-mode").textContent = te("execution_mode", mode, mode.toUpperCase());
      if ($("kpi-mode-note")) {
        $("kpi-mode-note").textContent = t(`app.modeNote.${mode === "live" ? "live" : "paper"}`, { broker: brokerName });
      }
      if ($("kpi-equity")) $("kpi-equity").textContent = fmtUsd(accountEquity, false);
      if ($("kpi-equity-note")) {
        $("kpi-equity-note").textContent = t("kpi.equity.noteValue", {
          cash: fmtUsd(cashBalance, false),
          positions: fmtUsd(positionsValue, false),
        });
      }
      if ($("kpi-cash")) $("kpi-cash").textContent = fmtUsd(cashBalance, false);
      if ($("kpi-cash-note")) {
        $("kpi-cash-note").textContent = t("kpi.cash.noteValue", {
          available: availableNotional.toFixed(2),
          snapshot: accountSnapshotLabel,
        });
      }
      if ($("kpi-pos")) $("kpi-pos").textContent = `${Number(summary.open_positions || 0)} / ${Number(summary.max_open_positions || 0)}`;
      if ($("kpi-slot-note")) {
        $("kpi-slot-note").textContent = t("kpi.positions.noteValue", {
          utilization: fmtPct(slotUtil, 2),
        });
      }
      if ($("kpi-notional")) $("kpi-notional").textContent = fmtUsd(trackedNotional, false);
      if ($("kpi-notional-note")) {
        $("kpi-notional-note").textContent = t("kpi.notional.noteValue", {
          signals: Number(summary.signals || 0),
          snapshot: accountSnapshotLabel,
        });
      }
      if ($("kpi-risk-mode")) {
        $("kpi-risk-mode").textContent = slotUtil >= Number(config.congested_utilization_threshold || 1) * 100
          ? t("kpi.risk.modeAdaptive")
          : t("kpi.risk.modeNormal");
      }
      if ($("kpi-risk-note")) {
        $("kpi-risk-note").textContent = t("kpi.risk.noteValue", {
          threshold: fmtPct(Number(config.congested_utilization_threshold || 0) * 100, 0),
          trim: fmtPct(Number(config.congested_trim_pct || 0) * 100, 0),
        });
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
        `<tr><td colspan="8">${escapeHtml(t("positions.table.empty"))}</td></tr>`
      );

      const orders = (data.orders || []).slice(0, 20);
      updateOrderChips(orders);
      replaceRows(
        $("orders-body"),
        orders.slice(0, 10).map((o) => {
          const st = String(o.status || "PENDING").toUpperCase();
          const map = {
            FILLED: ["ok", t("orders.status.filled")],
            PENDING: ["wait", t("orders.status.pending")],
            REJECTED: ["danger", t("orders.status.rejected")],
            CANCELED: ["cancel", t("orders.status.canceled")],
            CLEARED: ["cancel", t("orders.status.cleared")],
          };
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
        `<tr><td colspan="5">${escapeHtml(t("orders.table.empty"))}</td></tr>`
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
          const scoreDetail = t("wallets.monitor.row.detail", {
            positions: Number(w.positions || 0),
            markets: Number(w.unique_markets || 0),
            notional: Number(w.notional || 0).toFixed(0),
          });
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
                <span><span class="tag ${tierTagClass(tier)}">${walletTierLabel(tier)}</span></span>
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
        `<tr><td colspan="7">${escapeHtml(t("wallets.monitor.empty"))}</td></tr>`
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
            ? t("sources.availability.historyValue", {
              winRate: fmtRatioPct(s.win_rate, 0),
              roi: fmtSignedRatioPct(s.roi, 0),
            })
            : t("sources.availability.historyEmpty");
          const discoveryText = Number(s.discovery_priority_score || 0) > 0
            ? t("sources.availability.discoveryPool", {
              rank: Number(s.discovery_priority_rank || 0),
              score: Number(s.discovery_priority_score || 0).toFixed(2),
            })
            : t("sources.availability.discoveryEmpty");
          return `<tr>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${shortWallet(s.name)}</span>
                <span class="cell-sub">${t("sources.availability.sourceDetail", {
                  positions: Number(s.positions || 0),
                  markets: Number(s.unique_markets || 0),
                })}</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span class="${clsForScore(score)} cell-main">${score.toFixed(1)}</span>
                <span><span class="tag ${tierTagClass(tier)}">${walletTierLabel(tier)}</span></span>
              </div>
            </td>
            <td class="${clsForWeight(Number(s.weight || 0))}">${Number(s.weight || 0).toFixed(2)}</td>
            <td class="wrap">
              <div class="cell-stack">
                <span><span class="tag ${history.cls}">${history.label}</span></span>
                <span class="cell-sub">${Number(s.resolved_markets || 0) > 0 ? t("sources.availability.historyResolved", {
                  history: historyText,
                  resolved: fmtRatioPct(s.resolved_win_rate, 0),
                }) : historyText}</span>
              </div>
            </td>
            <td class="wrap">
              <div class="cell-stack">
                <span class="cell-main">${s.updated || "-"}</span>
                <span class="cell-sub">${t("sources.availability.updatedDetail", {
                  historyAge: historyAgeLabel(s.history_refresh_ts, now),
                  discovery: discoveryText,
                })}</span>
              </div>
            </td>
          </tr>`.replace("<tr>", `<tr class="${rowClass}" data-wallet="${walletKey}">`);
        }),
        `<tr><td colspan="5">${escapeHtml(t("sources.availability.empty"))}</td></tr>`
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
          : `<li><span class="level green">${t("alerts.emptyTag")}</span><span>${t("alerts.emptyMessage")}</span></li>`;
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
                ? ["ok", t("enum.pendingOrderStatus.FILLED")]
                : status === "REJECTED"
                  ? ["danger", t("orders.status.rejected")]
                  : status
                    ? ["wait", status]
                    : ["cancel", t("timeline.notRecorded")];
              return `<li><span>${t.time}</span><b><span class="tag ${actionCls}">${actionTag}</span> <span class="tag ${statusCls}">${statusTag}</span> ${titleText || rawText || ""}</b></li>`;
            }).join("")
          : `<li><span>${escapeHtml(t("common.dashTime"))}</span><b>${t("timeline.empty")}</b></li>`;
      }

      updateRiskMeters(summary, orders, config, stateAgeSec);
      if ($("health-runtime")) $("health-runtime").textContent = t("health.runtime", { age: fmtAge(stateAgeSec), poll: pollSec });
      if ($("health-state-age")) $("health-state-age").textContent = `${stateAgeSec}s`;
      if ($("health-broker")) $("health-broker").textContent = brokerName;
      if ($("health-wallet-pool")) $("health-wallet-pool").textContent = String(config.wallet_pool_size || 0);
      if ($("health-discovery")) {
        const enabled = config.wallet_discovery_enabled ? t("common.enabled") : t("common.disabled");
        $("health-discovery").textContent = t("health.discovery", { mode: String(config.wallet_discovery_mode || "-"), enabled });
      }
      if ($("health-control")) $("health-control").textContent = controlLabel();
    } catch (_err) {
      // keep static fallback
    }
  }

  async function init() {
    if (i18n && typeof i18n.init === "function") {
      await i18n.init();
      if (typeof i18n.translateDom === "function") {
        i18n.translateDom(document);
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
    await refresh();
    setInterval(refresh, FRONTEND_REFRESH_SECONDS * 1000);
  }

  init().catch(() => null);
})();
