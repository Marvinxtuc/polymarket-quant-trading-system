#!/bin/bash
set -euo pipefail

out="${1:-/tmp/poly_monitor_12h_report.txt}"
log="${2:-/tmp/poly_daemon.log}"
window_sec="${3:-43200}"
state_file="${4:-/tmp/poly_monitor_12h_inconclusive_state}"
daemon_state_file="${5:-/tmp/poly_runtime_data/state.json}"
json_out="${6:-/tmp/poly_monitor_12h_report.json}"
state_api_url="http://127.0.0.1:8787/api/state"

read_dotenv_var() {
  local key="$1"
  local dotenv="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.env"
  [[ -f "$dotenv" ]] || return 0
  awk -F= -v key="$key" '
    $0 ~ "^[[:space:]]*" key "=" {
      sub(/^[[:space:]]*[^=]+=/, "", $0)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0)
      gsub(/^"|"$/, "", $0)
      gsub(/^'\''|'\''$/, "", $0)
      print $0
      exit
    }
  ' "$dotenv"
}

append_control_token() {
  local url="$1"
  local token="${POLY_CONTROL_TOKEN:-$(read_dotenv_var POLY_CONTROL_TOKEN)}"
  if [[ -z "${token:-}" ]] || [[ "$url" == *"token="* ]]; then
    printf '%s\n' "$url"
    return 0
  fi
  if [[ "$url" == *"?"* ]]; then
    printf '%s&token=%s\n' "$url" "$token"
  else
    printf '%s?token=%s\n' "$url" "$token"
  fi
}

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

window_bounds="$(
  python3 - "$window_sec" <<'PY'
from datetime import datetime, timedelta
import sys

window_sec = int(sys.argv[1])
end = datetime.now()
start = end - timedelta(seconds=window_sec)
print(start.strftime("%Y-%m-%d %H:%M:%S"))
print(end.strftime("%Y-%m-%d %H:%M:%S"))
PY
)"
start_ts="$(printf '%s\n' "$window_bounds" | sed -n '1p')"
end_ts="$(printf '%s\n' "$window_bounds" | sed -n '2p')"
seg=""
if [[ -f "$log" ]]; then
  seg="$(
    python3 - "$log" "$window_sec" <<'PY'
from datetime import datetime, timedelta
import re
import sys

path = sys.argv[1]
window_sec = int(sys.argv[2])
end = datetime.now()
start = end - timedelta(seconds=window_sec)
line_re = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?:,\d+)?\b")

with open(path, "r", encoding="utf-8", errors="ignore") as fh:
    for raw in fh:
        match = line_re.match(raw)
        if not match:
            continue
        try:
            ts = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        if start <= ts <= end:
            sys.stdout.write(raw)
PY
  )"
fi

exec_cnt="$(printf "%s" "$seg" | rg -c ' EXEC ' || true)"
exec_cnt="${exec_cnt:-0}"
skip_max_open="$(printf "%s" "$seg" | rg -c 'SKIP .*reason=max open positions reached' || true)"
skip_max_open="${skip_max_open:-0}"
skip_add_cd="$(printf "%s" "$seg" | rg -c 'SKIP .*reason=token add cooldown' || true)"
skip_add_cd="${skip_add_cd:-0}"
time_exit_close="$(printf "%s" "$seg" | rg -c 'TIME_EXIT_CLOSE' || true)"
time_exit_close="${time_exit_close:-0}"
reject_cnt="$(printf "%s" "$seg" | rg -c 'FAIL wallet=' || true)"
reject_cnt="${reject_cnt:-0}"

ratio_skip_max_open="$(awk -v a="$skip_max_open" -v b="$exec_cnt" 'BEGIN{if(b==0)print "NA"; else printf "%.3f", a/b}')"
ratio_time_exit_close="$(awk -v a="$time_exit_close" -v b="$exec_cnt" 'BEGIN{if(b==0)print "NA"; else printf "%.3f", a/b}')"
ratio_skip_add_cd="$(awk -v a="$skip_add_cd" -v b="$exec_cnt" 'BEGIN{if(b==0)print "NA"; else printf "%.3f", a/b}')"
ratio_reject="$(awk -v a="$reject_cnt" -v b="$exec_cnt" 'BEGIN{if(b==0)print "NA"; else printf "%.3f", a/b}')"

if [[ "$exec_cnt" -eq 0 ]]; then
  sample_status="INCONCLUSIVE"
  ratio_skip_max_open="NA (no samples)"
  ratio_time_exit_close="NA (no samples)"
  ratio_skip_add_cd="NA (no samples)"
  ratio_reject="NA (no samples)"
  pass1="INCONCLUSIVE"
  pass2="INCONCLUSIVE"
  pass3="INCONCLUSIVE"
  pass4="INCONCLUSIVE"
  prev_inconclusive=0
  if [[ -f "$state_file" ]]; then
    prev_inconclusive="$(cat "$state_file" | tr -dc '0-9' || true)"
    prev_inconclusive="${prev_inconclusive:-0}"
  fi
  inconclusive_count=$((prev_inconclusive + 1))
  printf '%s' "$inconclusive_count" > "$state_file"
  if (( inconclusive_count >= 2 )); then
    recommendation="CONSECUTIVE_INCONCLUSIVE: last 2+ 12h windows had EXEC=0. Keep observing for 1 full 12h window before parameter changes."
  else
    recommendation="Observation started. Need 2 consecutive 12h INCONCLUSIVE windows to trigger escalation."
  fi
else
  sample_status="CONCLUSIVE"
  pass1="$(awk -v r="$ratio_skip_max_open" 'BEGIN{if(r<0.35) print "PASS"; else print "FAIL"}')"
  pass2="$(awk -v r="$ratio_time_exit_close" 'BEGIN{if(r>=0.25 && r<=0.70) print "PASS"; else print "FAIL"}')"
  pass3="$(awk -v r="$ratio_skip_add_cd" 'BEGIN{if(r<0.2) print "PASS"; else print "FAIL"}')"
  pass4="$(awk -v r="$ratio_reject" 'BEGIN{if(r<0.25) print "PASS"; else print "FAIL"}')"
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
resolved_state_payload="$(mktemp)"
cleanup_payload() {
  rm -f "$resolved_state_payload"
}
trap cleanup_payload EXIT
if ! curl -fsS --max-time 5 "$(append_control_token "$state_api_url")" > "$resolved_state_payload" 2>/dev/null; then
  if [[ -f "$daemon_state_file" ]]; then
    cp "$daemon_state_file" "$resolved_state_payload"
  else
    rm -f "$resolved_state_payload"
    resolved_state_payload=""
  fi
fi
if [[ -n "${resolved_state_payload:-}" && -f "$resolved_state_payload" ]]; then
  startup_ready="$(jq -r 'if .startup.ready == true then "true" elif .startup.ready == false then "false" else "unknown" end' "$resolved_state_payload" 2>/dev/null || printf 'unknown')"
  recon_status="$(jq -r '.reconciliation.status // "unknown"' "$resolved_state_payload" 2>/dev/null || printf 'unknown')"
  recon_issues="$(jq -r '(.reconciliation.issues // []) | if length == 0 then "(none)" else join("; ") end' "$resolved_state_payload" 2>/dev/null || printf '(state parse failed)')"
  recon_internal_ledger_diff="$(jq -r '.reconciliation.internal_vs_ledger_diff // "NA"' "$resolved_state_payload" 2>/dev/null || printf 'NA')"
  recon_broker_floor_gap="$(jq -r '.reconciliation.broker_floor_gap_vs_internal // "NA"' "$resolved_state_payload" 2>/dev/null || printf 'NA')"
  recon_stale_pending="$(jq -r '.reconciliation.stale_pending_orders // "NA"' "$resolved_state_payload" 2>/dev/null || printf 'NA')"
  recon_account_age="$(jq -r '.reconciliation.account_snapshot_age_seconds // "NA"' "$resolved_state_payload" 2>/dev/null || printf 'NA')"
  recon_broker_reconcile_age="$(jq -r '.reconciliation.broker_reconcile_age_seconds // "NA"' "$resolved_state_payload" 2>/dev/null || printf 'NA')"
  recon_event_age="$(jq -r '.reconciliation.broker_event_sync_age_seconds // "NA"' "$resolved_state_payload" 2>/dev/null || printf 'NA')"
  startup_json="$(jq -c '.startup // {}' "$resolved_state_payload" 2>/dev/null || printf '{}')"
  reconciliation_json="$(jq -c '.reconciliation // {}' "$resolved_state_payload" 2>/dev/null || printf '{}')"
fi

final_recommendation="$recommendation"
if [[ "$startup_ready" != "true" && "$startup_ready" != "unknown" ]]; then
  final_recommendation="BLOCK: startup self-check is not ready. Fix env / network smoke / broker prerequisites before tuning."
elif [[ "$recon_status" == "fail" ]]; then
  recon_focus="$recon_issues"
  if [[ -z "$recon_focus" || "$recon_focus" == "(none)" ]]; then
    recon_focus="review ledger drift and broker sync facts"
  fi
  final_recommendation="ESCALATE: reconciliation failed (${recon_focus}). Monitor: ${recommendation}"
elif [[ "$recon_status" == "warn" ]]; then
  recon_focus="$recon_issues"
  if [[ -z "$recon_focus" || "$recon_focus" == "(none)" ]]; then
    recon_focus="review pending orders and sync freshness"
  fi
  final_recommendation="OBSERVE: reconciliation has warnings (${recon_focus}). Monitor: ${recommendation}"
fi

cat >"$out" <<EOF
Polymarket Threshold Report (12h)
window_start: $start_ts
window_end:   $end_ts
window_seconds: $window_sec
log_file: $log

counts:
  EXEC: $exec_cnt
  SKIP(max open): $skip_max_open
  TIME_EXIT_CLOSE: $time_exit_close
  SKIP(token add cooldown): $skip_add_cd
  FAIL(wallet side error): $reject_cnt

sample_status: $sample_status

ratios:
  SKIP(max open)/EXEC: $ratio_skip_max_open (target < 0.35) => $pass1
  TIME_EXIT_CLOSE/EXEC: $ratio_time_exit_close (target 0.25~0.70) => $pass2
  SKIP(token add cooldown)/EXEC: $ratio_skip_add_cd (target < 0.20) => $pass3
  FAIL(wallet)/EXEC: $ratio_reject (target < 0.25) => $pass4

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
ratio_reject_json="$(json_number_or_null "$ratio_reject")"

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
  --argjson reject_cnt "$reject_cnt" \
  --argjson ratio_skip_max_open "$ratio_skip_max_open_json" \
  --argjson ratio_time_exit_close "$ratio_time_exit_close_json" \
  --argjson ratio_skip_add_cd "$ratio_skip_add_cd_json" \
  --argjson ratio_reject "$ratio_reject_json" \
  --argjson consecutive_inconclusive_windows "$inconclusive_count" \
  --argjson startup_ready "$startup_ready_json" \
  --argjson startup "$startup_json" \
  --argjson reconciliation "$reconciliation_json" \
  '{
    report_type: "monitor_12h",
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
      skip_token_add_cooldown: $skip_add_cd,
      reject_wallet_failures: $reject_cnt
    },
    ratios: {
      skip_max_open_per_exec: $ratio_skip_max_open,
      time_exit_close_per_exec: $ratio_time_exit_close,
      skip_token_add_cooldown_per_exec: $ratio_skip_add_cd,
      reject_wallet_failures_per_exec: $ratio_reject
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
sleep "$window_sec"
