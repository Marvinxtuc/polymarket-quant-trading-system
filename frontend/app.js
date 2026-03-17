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
      const res = await fetch("/api/state", { cache: "no-store" });
      if (!res.ok) return;
      const data = await res.json();

      const summary = data.summary || {};
      const config = data.config || {};
      const control = data.control || {};
      const now = Number(data.ts || 0);
      const pollSec = Number(config.poll_interval_seconds || 0);
      const stateAgeSec = Math.max(0, Math.floor(Date.now() / 1000 - now));
      const slotUtil = Number(summary.slot_utilization_pct || summary.exposure_pct || 0);
      const mode = String(config.execution_mode || (config.dry_run ? "paper" : "live")).toLowerCase();
      const brokerName = String(config.broker_name || (mode === "live" ? "LiveClobBroker" : "PaperBroker"));
      renderControlState(control);
      updateModeBadge(config);

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
          const map = { FILLED: ["ok", "已成交"], PENDING: ["wait", "待成交"], REJECTED: ["danger", "已拒绝"], CANCELED: ["cancel", "已撤单"] };
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
  bindWalletSelection();
  bindExitReviewFilters();
  bindExitReviewSamples();
  bindTraceReviewInteractions();
  bindAttributionWindows();
  refresh();
  setInterval(refresh, FRONTEND_REFRESH_SECONDS * 1000);
})();
