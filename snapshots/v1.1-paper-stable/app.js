(function () {
  const $ = (id) => document.getElementById(id);

  function fmtUsd(n) {
    const v = Number(n || 0);
    const sign = v > 0 ? "+" : "";
    return `${sign}$${v.toFixed(2)}`;
  }

  function fmtPct(n, digits = 2) {
    return `${Number(n || 0).toFixed(digits)}%`;
  }

  function hhmm(ts) {
    const d = new Date((ts || 0) * 1000);
    const hh = String(d.getHours()).padStart(2, "0");
    const mm = String(d.getMinutes()).padStart(2, "0");
    const ss = String(d.getSeconds()).padStart(2, "0");
    return `${hh}:${mm}:${ss}`;
  }

  function replaceRows(tbody, rows) {
    if (!tbody) return;
    tbody.innerHTML = rows.join("");
  }

  function clsForWeight(w) {
    if (w >= 0.8) return "w-high";
    if (w >= 0.6) return "w-mid";
    return "w-low";
  }

  function updateStrategyParams(config) {
    if ($("param-wallet-pool")) $("param-wallet-pool").textContent = `wallet_pool=${Number(config.wallet_pool_size || 0)}`;
    if ($("param-min-increase")) $("param-min-increase").textContent = `${Number(config.min_wallet_increase_usd || 0).toFixed(0)} USD`;
    if ($("param-max-signals")) $("param-max-signals").textContent = String(config.max_signals_per_cycle || 0);
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

  function updateRiskMeters(summary, orders, config) {
    const used = Number(summary.daily_loss_used_pct || 0);
    const exposure = Number(summary.exposure_pct || 0);
    const maxOpen = Number(summary.max_open_positions || 0);
    const openPos = Number(summary.open_positions || 0);

    let cooldownSkips = 0;
    for (const o of orders) {
      if (String(o.reason || "").includes("token add cooldown")) cooldownSkips += 1;
    }
    const cooldownRate = orders.length > 0 ? (cooldownSkips / orders.length) * 100 : 0;

    if ($("meter-dd")) $("meter-dd").value = Math.max(0, Math.min(100, used));
    if ($("meter-dd-text")) $("meter-dd-text").textContent = `${fmtPct(used, 1)} / ${fmtPct(Number(config.daily_max_loss_pct || 0) * 100, 1)}`;

    if ($("meter-exp")) $("meter-exp").value = Math.max(0, Math.min(100, exposure));
    if ($("meter-exp-text")) $("meter-exp-text").textContent = `${fmtPct(exposure, 1)} / 100%`;

    if ($("meter-cooldown")) $("meter-cooldown").value = Math.max(0, Math.min(100, cooldownRate));
    if ($("meter-cooldown-text")) $("meter-cooldown-text").textContent = `${cooldownSkips} / ${orders.length}`;

    const util = maxOpen > 0 ? (openPos / maxOpen) * 100 : 0;
    if ($("meter-util")) $("meter-util").value = Math.max(0, Math.min(100, util));
    if ($("meter-util-text")) $("meter-util-text").textContent = `${fmtPct(util, 1)} / 100%`;
  }

  async function refresh() {
    try {
      const res = await fetch("/api/state", { cache: "no-store" });
      if (!res.ok) return;
      const data = await res.json();

      const summary = data.summary || {};
      const config = data.config || {};
      const now = Number(data.ts || 0);
      const pollSec = Number(config.poll_interval_seconds || 0);

      if ($("status-line")) {
        $("status-line").textContent = `聪明钱包共识引擎 · 最近刷新 ${hhmm(now)} · 下次刷新 ${pollSec}s · 每 ${pollSec}s 轮询`;
      }

      if ($("guard-line")) {
        const bankroll = Number(config.bankroll_usd || 0);
        const perTrade = Number(summary.per_trade_notional || 0);
        const remainRisk = Number(summary.daily_loss_remaining_pct || 0);
        $("guard-line").textContent = `账户 ${bankroll.toFixed(0)}U · 单笔上限 ${perTrade.toFixed(2)}U · 今日剩余风险 ${fmtPct(remainRisk, 1)}`;
      }

      if ($("budget-meta")) {
        const used = Number(summary.daily_loss_used_pct || 0);
        const remain = Number(summary.daily_loss_remaining_pct || 0);
        const est = Number(summary.est_openings || 0);
        $("budget-meta").textContent = `已使用 ${fmtPct(used, 1)} · 剩余 ${fmtPct(remain, 1)} · 预计可开仓 ${est} 次`;
      }
      if ($("budget-fill")) {
        const used = Number(summary.daily_loss_used_pct || 0);
        $("budget-fill").style.width = `${Math.max(0, Math.min(100, used)).toFixed(1)}%`;
      }

      if ($("kpi-pnl")) $("kpi-pnl").textContent = fmtUsd(summary.pnl_today || 0);
      if ($("kpi-equity")) $("kpi-equity").textContent = `权益：${Number(summary.equity || 0).toFixed(2)} USDC`;
      if ($("kpi-pos")) $("kpi-pos").textContent = `${Number(summary.open_positions || 0)} / ${Number(summary.max_open_positions || 0)}`;
      if ($("kpi-exp")) $("kpi-exp").textContent = `总敞口：${fmtPct(summary.exposure_pct || 0, 2)}`;
      if ($("kpi-signals")) $("kpi-signals").textContent = String(summary.signals || 0);
      if ($("kpi-signals-note")) $("kpi-signals-note").textContent = `每轮最多 ${Number(config.max_signals_per_cycle || 0)} 条`;
      if ($("kpi-risk-mode")) $("kpi-risk-mode").textContent = Number(summary.exposure_pct || 0) >= Number(config.congested_utilization_threshold || 1) * 100 ? "拥堵自适应" : "常规风控";
      if ($("kpi-risk-note")) $("kpi-risk-note").textContent = `拥堵阈值 ${fmtPct(Number(config.congested_utilization_threshold || 0) * 100, 0)} / 拥堵减仓 ${fmtPct(Number(config.congested_trim_pct || 0) * 100, 0)}`;

      updateStrategyParams(config);

      const positions = (data.positions || []).slice(0, 8);
      replaceRows(
        $("positions-body"),
        positions.map((p) => {
          const unreal = Number(p.unrealized_pnl || 0);
          const edge = Number(p.edge_pct || 0);
          const holdMin = Math.max(1, Math.floor((Date.now() / 1000 - Number(p.opened_ts || now)) / 60));
          return `<tr>
            <td>${p.title || p.market_slug || "-"}</td>
            <td>${p.outcome || "YES"}</td>
            <td>${Number(p.quantity || 0).toFixed(2)}</td>
            <td>${holdMin}m</td>
            <td>${holdMin < 1440 ? '<span class="warn">18h</span>' : '2d+'}</td>
            <td class="${edge >= 0 ? "good" : "bad"}">${edge >= 0 ? "+" : ""}${edge.toFixed(2)}%</td>
            <td class="${unreal >= 0 ? "good" : "bad"}">${fmtUsd(unreal)}</td>
            <td>${p.reason || "-"}</td>
            <td>${p.exit_rule || "-"}</td>
          </tr>`;
        })
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
          return `<tr>
            <td class="mono">${ts}</td>
            <td>${o.title || "-"}</td>
            <td>${o.side === "BUY" ? "买入" : "卖出"}</td>
            <td><span class="tag ${cls}">${txt}</span></td>
            <td>${Number(o.retry_count || 0)}/3</td>
            <td>${Number(o.latency_ms || 0)}ms</td>
            <td>${o.reason || "-"}</td>
          </tr>`;
        })
      );

      const wallets = (data.wallets || []).slice(0, 8);
      replaceRows(
        $("wallets-body"),
        wallets.map((w) => {
          const n = Number(w.notional || 0);
          const trades = Number(w.positions || 0);
          const pnl = Number(w.daily_pnl || 0);
          const pnlCls = pnl >= 0 ? "good" : "bad";
          const statusClass = String(w.status || "在线") === "在线" ? "ok" : "wait";
          return `<tr>
            <td>${String(w.wallet || "").slice(0, 8)}...</td>
            <td>${n.toFixed(1)}</td>
            <td class="${pnlCls}">${pnl >= 0 ? "+" : ""}${pnl.toFixed(1)}</td>
            <td>${trades}</td>
            <td><span class="tag ${statusClass}">${w.status || "在线"}</span></td>
            <td>${w.note || "-"}</td>
          </tr>`;
        })
      );

      const sources = (data.sources || []).slice(0, 8);
      replaceRows(
        $("sources-body"),
        sources.map((s) => `<tr>
          <td>${String(s.name || "").slice(0, 8)}...</td>
          <td class="${clsForWeight(Number(s.weight || 0))}">${Number(s.weight || 0).toFixed(2)}</td>
          <td><span class="tag ${s.status === "在线" ? "ok" : "danger"}">${s.status || "在线"}</span></td>
          <td>${s.updated || "-"}</td>
          <td>${s.hit_rate || "-"}</td>
        </tr>`)
      );

      const alerts = (data.alerts || []).slice(0, 6);
      if ($("alerts-list")) {
        $("alerts-list").innerHTML = alerts.map((a) => `<li><span class="level ${a.cls}">${a.tag}</span><span>${a.message}</span></li>`).join("");
      }

      const timeline = (data.timeline || []).slice(0, 8);
      if ($("timeline-list")) {
        $("timeline-list").innerHTML = timeline.map((t) => `<li><span>${t.time}</span><b>${t.text}</b></li>`).join("");
      }

      updateRiskMeters(summary, orders, config);
      if ($("health-runtime")) $("health-runtime").textContent = `轮询 ${pollSec}s · 最近刷新 ${hhmm(now)}`;
      if ($("health-dataapi")) $("health-dataapi").textContent = config.wallet_discovery_enabled ? "enabled" : "disabled";
      if ($("health-polyapi")) $("health-polyapi").textContent = "online";
      if ($("health-wallet-pool")) $("health-wallet-pool").textContent = String(config.wallet_pool_size || 0);
      if ($("health-discovery")) $("health-discovery").textContent = String(config.wallet_discovery_mode || "-");
      if ($("health-gateway")) $("health-gateway").textContent = "正常";
    } catch (_err) {
      // keep static fallback
    }
  }

  refresh();
  setInterval(refresh, 5000);
})();
