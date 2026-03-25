(function () {
  const cache = Object.create(null);
  const DEFAULT_LOCALE = "zh-CN";
  const STORAGE_KEY = "poly.locale";
  const KEY_ALIASES = {
    "nav.overview": "nav.workspace.overview",
    "candidate.filter.expand": "candidate.panel.toggleFiltersExpand",
    "candidate.filter.collapse": "candidate.panel.toggleFiltersCollapse",
    "candidate.status.loading": "common.waitingData",
    "blockbeats.meta": "blockbeats.panel.meta",
    "blockbeats.status.line": "blockbeats.panel.status",
    "blockbeats.status.issuesSuffix": "blockbeats.panel.statusIssues",
    "blockbeats.summary.overallStatus": "blockbeats.summary.status",
    "notifier.meta": "notifier.panel.meta",
    "notifier.status.configured": "notifier.panel.statusConfigured",
    "notifier.status.noneConfigured": "notifier.panel.statusNone",
    "notifier.recent.deliveries": "notifier.list.deliveries",
    "notifier.recent.empty": "notifier.list.recentNone",
    "journal.meta": "journal.panel.meta",
    "journal.status.apiError": "journal.panel.statusError",
    "journal.status.ready": "journal.panel.statusReady",
    "journal.status.loading": "journal.panel.waiting",
    "journal.note.help": "journal.composer.ready",
    "journal.note.missing": "journal.card.noNote",
    "journal.empty.noneTitle": "journal.card.emptyTitle",
    "journal.empty.noneBody": "journal.card.emptyBody",
    "journal.empty.noneHelp": "journal.card.emptyBody",
    "journal.empty.apiUnavailableTitle": "journal.card.title",
    "journal.empty.apiUnavailableBody": "journal.card.apiUnavailable",
    "walletProfiles.status.apiError": "walletProfiles.status.error",
    "walletProfiles.status.loading": "common.waitingData",
    "walletProfiles.empty.apiUnavailable": "walletProfiles.fallback.apiUnavailable",
    "walletProfiles.empty.none": "walletProfiles.fallback.empty",
    "decision.meta": "candidate.panel.meta",
    "decision.gridMeta": "candidate.panel.metaVisible",
    "decision.hasOpportunities": "candidate.panel.statusReady",
    "decision.noOpportunities": "candidate.panel.statusEmpty",
    "control.pauseOpening": "app.control.pauseOpening.enable",
    "control.resumeOpening": "app.control.pauseOpening.disable",
    "control.reduceOnly": "app.control.reduceOnly.enable",
    "control.clearReduceOnly": "app.control.reduceOnly.disable",
    "control.emergencyStop": "app.control.emergencyStop.enable",
    "control.clearEmergencyStop": "app.control.emergencyStop.disable",
    "enum.candidate_action.follow_sell": "enum.candidateAction.follow_sell",
    "enum.candidate_action.handle": "enum.candidateAction.default",
    "enum.candidate_action.ignore": "enum.candidateAction.ignore",
    "enum.candidate_action.watch": "enum.candidateAction.watch"
  };
  const GROUP_ALIASES = {
    workspace_view: "enum.workspaceView",
    candidate_focus_view: "enum.candidateFocusView",
    decision_mode: "enum.mode",
    execution_mode: "enum.mode",
    control_state: "enum.controlState",
    candidate_status: "enum.candidateStatus",
    candidate_action: "enum.candidateAction",
    report_status: "enum.reportStatus",
    blockbeats_source: "enum.blockbeatsSource",
    exit_kind: "enum.exitKind",
    exit_result: "enum.exitResult",
    action_tag: "enum.actionTag",
    signal_status: "enum.signalStatus",
    trace_status: "enum.traceStatus",
    pending_order_status: "enum.pendingOrderStatus",
    operator_action: "enum.operatorAction",
    ops_gate_level: "enum.opsGateLevel",
    reason: "enum.reason",
    notification_channel: "common.channel"
  };
  let current = DEFAULT_LOCALE;

  function readKey(source, key) {
    return String(key || "")
      .split(".")
      .filter(Boolean)
      .reduce((node, part) => (node && typeof node === "object" ? node[part] : undefined), source);
  }

  function format(template, vars) {
    const values = vars && typeof vars === "object" ? vars : {};
    return String(template || "").replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_match, name) => {
      if (!Object.prototype.hasOwnProperty.call(values, name)) return "";
      return String(values[name]);
    });
  }

  function keyVariants(key) {
    const normalized = String(key || "").trim();
    if (!normalized) return [];
    const seen = new Set();
    const variants = [];
    const push = (value) => {
      const next = String(value || "").trim();
      if (!next || seen.has(next)) return;
      seen.add(next);
      variants.push(next);
    };
    push(normalized);
    push(KEY_ALIASES[normalized]);
    return variants;
  }

  function pendingOrderVariant(value) {
    const normalized = String(value || "").trim().toLowerCase();
    if (!normalized) return "UNKNOWN";
    if (["filled", "matched", "confirmed"].includes(normalized)) return "FILLED";
    if (["partially_filled", "partial_fill", "delayed", "partial"].includes(normalized)) return "PARTIAL";
    if (["live", "pending", "submitted", "accepted"].includes(normalized)) return "LIVE";
    if (["canceled", "cancelled", "unmatched"].includes(normalized)) return "CANCELED";
    if (["failed", "rejected", "error"].includes(normalized)) return "FAILED";
    return normalized.toUpperCase();
  }

  function enumValue(group, value) {
    const normalized = String(value == null ? "" : value).trim();
    if (!normalized) return "";
    if (group === "pending_order_status") return pendingOrderVariant(normalized);
    if (group === "trace_status") return normalized.toLowerCase() === "active" ? "open" : normalized.toLowerCase();
    if (group === "operator_action") return normalized.toUpperCase();
    if (group === "candidate_action" && normalized.toLowerCase() === "handle") return "default";
    if (group === "signal_status") return normalized.toLowerCase();
    if (group === "candidate_status") return normalized.toLowerCase();
    if (group === "report_status") return normalized.toLowerCase();
    if (group === "blockbeats_source") return normalized.toLowerCase();
    if (group === "decision_mode" || group === "execution_mode" || group === "control_state" || group === "ops_gate_level") {
      return normalized.toLowerCase();
    }
    return normalized;
  }

  async function loadMessages(locale) {
    const key = String(locale || DEFAULT_LOCALE).trim() || DEFAULT_LOCALE;
    if (cache[key]) return cache[key];
    const response = await fetch(`./locales/${encodeURIComponent(key)}.json`, { cache: "no-store" });
    if (!response.ok) throw new Error(`locale ${key} unavailable`);
    const payload = await response.json();
    cache[key] = payload && typeof payload === "object" ? payload : {};
    return cache[key];
  }

  function t(key, vars, fallback) {
    const locales = [current, DEFAULT_LOCALE, "en"];
    const variants = keyVariants(key);
    for (const locale of locales) {
      const messages = cache[locale];
      for (const variant of variants) {
        const value = readKey(messages, variant);
        if (typeof value === "string") return format(value, vars);
      }
    }
    return fallback || key;
  }

  function te(group, value, fallback) {
    const normalized = enumValue(group, value);
    if (!normalized) return fallback || "";
    const prefix = GROUP_ALIASES[group] || group;
    return t(`${prefix}.${normalized}`, {}, fallback || normalized);
  }

  function applyI18nValue(node, key, attr) {
    const resolved = t(key, {}, "");
    if (!resolved) return;
    if (attr) node.setAttribute(attr, resolved);
    else node.textContent = resolved;
  }

  function translateDom(root) {
    const target = root || document;
    target.querySelectorAll("[data-i18n]").forEach((node) => {
      applyI18nValue(node, node.getAttribute("data-i18n"), "");
    });
    target.querySelectorAll("[data-i18n-placeholder]").forEach((node) => {
      applyI18nValue(node, node.getAttribute("data-i18n-placeholder"), "placeholder");
    });
    target.querySelectorAll("[data-i18n-title]").forEach((node) => {
      applyI18nValue(node, node.getAttribute("data-i18n-title"), "title");
    });
    const titleNode = document.querySelector("title[data-i18n]");
    if (titleNode) {
      document.title = t(titleNode.getAttribute("data-i18n"), {}, document.title);
    }
  }

  async function init(options) {
    const config = options && typeof options === "object" ? options : {};
    const fallbackLocale = String(config.defaultLocale || DEFAULT_LOCALE).trim() || DEFAULT_LOCALE;
    const storedLocale = window.localStorage.getItem(config.storageKey || STORAGE_KEY);
    current = String(config.locale || storedLocale || fallbackLocale).trim() || fallbackLocale;
    try {
      await loadMessages(current);
    } catch (_err) {
      current = fallbackLocale;
      await loadMessages(current);
    }
    document.documentElement.lang = current;
    window.localStorage.setItem(config.storageKey || STORAGE_KEY, current);
    return cache[current];
  }

  async function setLocale(locale) {
    current = String(locale || DEFAULT_LOCALE).trim() || DEFAULT_LOCALE;
    await loadMessages(current);
    document.documentElement.lang = current;
    window.localStorage.setItem(STORAGE_KEY, current);
    translateDom(document);
    return current;
  }

  const api = {
    init,
    loadMessages,
    setLocale,
    translateDom,
    t,
    te,
    locale: function () {
      return current;
    },
    format
  };
  window.I18n = api;
  window.PolyI18n = api;
})();
