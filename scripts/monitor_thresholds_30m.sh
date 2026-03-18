#!/usr/bin/env bash
set -euo pipefail

out="${1:-/tmp/poly_monitor_30m_report.txt}"
log="${2:-/tmp/poly_daemon.log}"
window_sec="${3:-1800}"
state_file="${4:-/tmp/poly_monitor_30m_inconclusive_state}"
daemon_state_file="${5:-/tmp/poly_runtime_data/state.json}"
json_out="${6:-/tmp/poly_monitor_30m_report.json}"

json_number_or_null() {
  local value="$1"
  if [[ -z "$value" || "$value" == "NA" || "$value" == "unknown" ]]; then
    printf 'null'
    return
  fi
  if [[ "$value" == *"NA ("* ]]; then
    printf 'null'
    return
  fi
  printf '%s' "$value"
}

start_ts="$(date '+%Y-%m-%d %H:%M:%S')"
start_size=0
if [[ -f "$log" ]]; then
  start_size="$(wc -c < "$log")"
fi

sleep "$window_sec"

end_ts="$(date '+%Y-%m-%d %H:%M:%S')"
seg=""
if [[ -f "$log" ]]; then
  seg="$(tail -c +$((start_size + 1)) "$log" 2>/dev/null || true)"
fi

exec_cnt="$(printf "%s" "$seg" | rg -c ' EXEC ' || true)"
exec_cnt="${exec_cnt:-0}"
skip_max_open="$(printf "%s" "$seg" | rg -c 'SKIP .*reason=max open positions reached' || true)"
skip_max_open="${skip_max_open:-0}"
skip_add_cd="$(printf "%s" "$seg" | rg -c 'SKIP .*reason=token add cooldown' || true)"
skip_add_cd="${skip_add_cd:-0}"
time_exit_close="$(printf "%s" "$seg" | rg -c 'TIME_EXIT_CLOSE' || true)"
time_exit_close="${time_exit_close:-0}"

ratio_skip_max_open="$(awk -v a="$skip_max_open" -v b="$exec_cnt" 'BEGIN{if(b==0)print "NA"; else printf "%.3f", a/b}')"
ratio_time_exit_close="$(awk -v a="$time_exit_close" -v b="$exec_cnt" 'BEGIN{if(b==0)print "NA"; else printf "%.3f", a/b}')"
ratio_skip_add_cd="$(awk -v a="$skip_add_cd" -v b="$exec_cnt" 'BEGIN{if(b==0)print "NA"; else printf "%.3f", a/b}')"

if [[ "$exec_cnt" -eq 0 ]]; then
  sample_status="INCONCLUSIVE"
  ratio_skip_max_open="NA (no samples)"
  ratio_time_exit_close="NA (no samples)"
  ratio_skip_add_cd="NA (no samples)"
  pass1="INCONCLUSIVE"
  pass2="INCONCLUSIVE"
  pass3="INCONCLUSIVE"
  prev_inconclusive=0
  if [[ -f "$state_file" ]]; then
    prev_inconclusive="$(cat "$state_file" | tr -dc '0-9' || true)"
    prev_inconclusive="${prev_inconclusive:-0}"
  fi
  inconclusive_count=$((prev_inconclusive + 1))
  printf '%s' "$inconclusive_count" > "$state_file"
  if (( inconclusive_count >= 2 )); then
    recommendation="CONSECUTIVE_INCONCLUSIVE: last 2+ windows had EXEC=0. Keep observing for 1 full 30m window before parameter changes."
  else
    recommendation="Observation started. Need 2 consecutive INCONCLUSIVE windows to trigger escalation."
  fi
else
  sample_status="CONCLUSIVE"
  pass1="$(awk -v r="$ratio_skip_max_open" 'BEGIN{if(r<0.4) print "PASS"; else print "FAIL"}')"
  pass2="$(awk -v r="$ratio_time_exit_close" 'BEGIN{if(r>=0.3 && r<=0.8) print "PASS"; else print "FAIL"}')"
  pass3="$(awk -v r="$ratio_skip_add_cd" 'BEGIN{if(r<0.25) print "PASS"; else print "FAIL"}')"
  printf '0' > "$state_file"
  inconclusive_count=0
  recommendation="No escalation from sample rule."
fi

startup_ready="unknown"
recon_status="unknown"
recon_issues="(state unavailable)"
recon_internal_ledger_diff="NA"
recon_broker_floor_gap="NA"
recon_stale_pending="NA"
recon_account_age="NA"
recon_broker_reconcile_age="NA"
recon_event_age="NA"
startup_json='{}'
reconciliation_json='{}'
if [[ -f "$daemon_state_file" ]]; then
  startup_ready="$(jq -r 'if .startup.ready == true then "true" elif .startup.ready == false then "false" else "unknown" end' "$daemon_state_file" 2>/dev/null || printf 'unknown')"
  recon_status="$(jq -r '.reconciliation.status // "unknown"' "$daemon_state_file" 2>/dev/null || printf 'unknown')"
  recon_issues="$(jq -r '(.reconciliation.issues // []) | if length == 0 then "(none)" else join("; ") end' "$daemon_state_file" 2>/dev/null || printf '(state parse failed)')"
  recon_internal_ledger_diff="$(jq -r '.reconciliation.internal_vs_ledger_diff // "NA"' "$daemon_state_file" 2>/dev/null || printf 'NA')"
  recon_broker_floor_gap="$(jq -r '.reconciliation.broker_floor_gap_vs_internal // "NA"' "$daemon_state_file" 2>/dev/null || printf 'NA')"
  recon_stale_pending="$(jq -r '.reconciliation.stale_pending_orders // "NA"' "$daemon_state_file" 2>/dev/null || printf 'NA')"
  recon_account_age="$(jq -r '.reconciliation.account_snapshot_age_seconds // "NA"' "$daemon_state_file" 2>/dev/null || printf 'NA')"
  recon_broker_reconcile_age="$(jq -r '.reconciliation.broker_reconcile_age_seconds // "NA"' "$daemon_state_file" 2>/dev/null || printf 'NA')"
  recon_event_age="$(jq -r '.reconciliation.broker_event_sync_age_seconds // "NA"' "$daemon_state_file" 2>/dev/null || printf 'NA')"
  startup_json="$(jq -c '.startup // {}' "$daemon_state_file" 2>/dev/null || printf '{}')"
  reconciliation_json="$(jq -c '.reconciliation // {}' "$daemon_state_file" 2>/dev/null || printf '{}')"
fi

final_recommendation="$recommendation"
if [[ "$startup_ready" != "true" && "$startup_ready" != "unknown" ]]; then
  final_recommendation="BLOCK: startup self-check is not ready. Fix env / network smoke / broker prerequisites before tuning."
elif [[ "$recon_status" == "fail" ]]; then
  final_recommendation="ESCALATE: reconciliation failed. Investigate ledger drift or stale broker sync before strategy changes."
elif [[ "$recon_status" == "warn" ]]; then
  final_recommendation="OBSERVE: reconciliation has warnings. Review pending orders and sync freshness before parameter changes."
fi

cat >"$out" <<EOF
Polymarket Threshold Report
window_start: $start_ts
window_end:   $end_ts
window_seconds: $window_sec
log_file: $log

counts:
  EXEC: $exec_cnt
  SKIP(max open): $skip_max_open
  TIME_EXIT_CLOSE: $time_exit_close
  SKIP(token add cooldown): $skip_add_cd

sample_status: $sample_status

ratios:
  SKIP(max open)/EXEC: $ratio_skip_max_open (target < 0.4) => $pass1
  TIME_EXIT_CLOSE/EXEC: $ratio_time_exit_close (target 0.3~0.8) => $pass2
  SKIP(token add cooldown)/EXEC: $ratio_skip_add_cd (target < 0.25) => $pass3

recommendation: $recommendation

daemon_state_file: $daemon_state_file
startup_ready: $startup_ready
reconciliation:
  status: $recon_status
  internal_vs_ledger_diff: $recon_internal_ledger_diff
  broker_floor_gap_vs_internal: $recon_broker_floor_gap
  stale_pending_orders: $recon_stale_pending
  account_snapshot_age_seconds: $recon_account_age
  broker_reconcile_age_seconds: $recon_broker_reconcile_age
  broker_event_sync_age_seconds: $recon_event_age
  issues: $recon_issues

final_recommendation: $final_recommendation

consecutive_inconclusive_windows: $inconclusive_count
EOF

startup_ready_json='null'
if [[ "$startup_ready" == "true" ]]; then
  startup_ready_json='true'
elif [[ "$startup_ready" == "false" ]]; then
  startup_ready_json='false'
fi

ratio_skip_max_open_json="$(json_number_or_null "$ratio_skip_max_open")"
ratio_time_exit_close_json="$(json_number_or_null "$ratio_time_exit_close")"
ratio_skip_add_cd_json="$(json_number_or_null "$ratio_skip_add_cd")"

jq -n \
  --arg window_start "$start_ts" \
  --arg window_end "$end_ts" \
  --arg log_file "$log" \
  --arg sample_status "$sample_status" \
  --arg recommendation "$recommendation" \
  --arg final_recommendation "$final_recommendation" \
  --arg daemon_state_file "$daemon_state_file" \
  --arg recon_status "$recon_status" \
  --arg recon_issues "$recon_issues" \
  --argjson generated_ts "$(date +%s)" \
  --argjson window_seconds "$window_sec" \
  --argjson exec_cnt "$exec_cnt" \
  --argjson skip_max_open "$skip_max_open" \
  --argjson time_exit_close "$time_exit_close" \
  --argjson skip_add_cd "$skip_add_cd" \
  --argjson ratio_skip_max_open "$ratio_skip_max_open_json" \
  --argjson ratio_time_exit_close "$ratio_time_exit_close_json" \
  --argjson ratio_skip_add_cd "$ratio_skip_add_cd_json" \
  --argjson consecutive_inconclusive_windows "$inconclusive_count" \
  --argjson startup_ready "$startup_ready_json" \
  --argjson startup "$startup_json" \
  --argjson reconciliation "$reconciliation_json" \
  '{
    report_type: "monitor_30m",
    generated_ts: $generated_ts,
    window_start: $window_start,
    window_end: $window_end,
    window_seconds: $window_seconds,
    log_file: $log_file,
    sample_status: $sample_status,
    counts: {
      exec: $exec_cnt,
      skip_max_open: $skip_max_open,
      time_exit_close: $time_exit_close,
      skip_token_add_cooldown: $skip_add_cd
    },
    ratios: {
      skip_max_open_per_exec: $ratio_skip_max_open,
      time_exit_close_per_exec: $ratio_time_exit_close,
      skip_token_add_cooldown_per_exec: $ratio_skip_add_cd
    },
    recommendation: $recommendation,
    final_recommendation: $final_recommendation,
    consecutive_inconclusive_windows: $consecutive_inconclusive_windows,
    daemon_state_file: $daemon_state_file,
    startup_ready: $startup_ready,
    startup: $startup,
    reconciliation_status: $recon_status,
    reconciliation_issue_summary: $recon_issues,
    reconciliation: $reconciliation
  }' > "$json_out"

echo "$out"
