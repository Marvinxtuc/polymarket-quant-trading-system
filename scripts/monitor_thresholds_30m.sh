#!/usr/bin/env bash
set -euo pipefail

out="${1:-/tmp/poly_monitor_30m_report.txt}"
log="${2:-/tmp/poly_daemon.log}"
window_sec="${3:-1800}"

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
skip_max_open="$(printf "%s" "$seg" | rg -c 'SKIP .*reason=max open positions reached' || true)"
skip_add_cd="$(printf "%s" "$seg" | rg -c 'SKIP .*reason=token add cooldown' || true)"
time_exit_close="$(printf "%s" "$seg" | rg -c 'TIME_EXIT_CLOSE' || true)"

ratio_skip_max_open="$(awk -v a="$skip_max_open" -v b="$exec_cnt" 'BEGIN{if(b==0)print "NA"; else printf "%.3f", a/b}')"
ratio_time_exit_close="$(awk -v a="$time_exit_close" -v b="$exec_cnt" 'BEGIN{if(b==0)print "NA"; else printf "%.3f", a/b}')"
ratio_skip_add_cd="$(awk -v a="$skip_add_cd" -v b="$exec_cnt" 'BEGIN{if(b==0)print "NA"; else printf "%.3f", a/b}')"

pass1="$(awk -v r="$ratio_skip_max_open" 'BEGIN{if(r=="NA")print "N/A"; else if(r<0.4) print "PASS"; else print "FAIL"}')"
pass2="$(awk -v r="$ratio_time_exit_close" 'BEGIN{if(r=="NA")print "N/A"; else if(r>=0.3 && r<=0.8) print "PASS"; else print "FAIL"}')"
pass3="$(awk -v r="$ratio_skip_add_cd" 'BEGIN{if(r=="NA")print "N/A"; else if(r<0.25) print "PASS"; else print "FAIL"}')"

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

ratios:
  SKIP(max open)/EXEC: $ratio_skip_max_open (target < 0.4) => $pass1
  TIME_EXIT_CLOSE/EXEC: $ratio_time_exit_close (target 0.3~0.8) => $pass2
  SKIP(token add cooldown)/EXEC: $ratio_skip_add_cd (target < 0.25) => $pass3
EOF

echo "$out"
