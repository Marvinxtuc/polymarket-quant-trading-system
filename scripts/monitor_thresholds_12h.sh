#!/bin/bash
set -euo pipefail

out="${1:-/tmp/poly_monitor_12h_report.txt}"
log="${2:-/tmp/poly_daemon.log}"
window_sec="${3:-43200}"
state_file="${4:-/tmp/poly_monitor_12h_inconclusive_state}"

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
    recommendation="CONSECUTIVE_INCONCLUSIVE: last 2+ windows had EXEC=0. Keep observing for 1 full 30m window before parameter changes."
  else
    recommendation="Observation started. Need 2 consecutive INCONCLUSIVE windows to trigger escalation."
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

consecutive_inconclusive_windows: $inconclusive_count
EOF

echo "$out"
