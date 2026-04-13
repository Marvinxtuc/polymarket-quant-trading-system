.PHONY: venv install env-check test verify-stack verify one-click verify_one_click run-once run start-stack replay replay-calibrate reconciliation-report full-validate fault-drill release-gate readiness-brief rehearsal-finalize stop-stack ops-12h rehearse-10h rehearse-12h rehearse-24h rehearse-24h-dry-run rehearse-progress rehearse-24h-progress monitor-12h monitor-30m monitor-reports stop-monitor-reports monitor-scheduler-install monitor-scheduler-uninstall monitor-scheduler-status monitor-scheduler-smoke alert-smoke alert-smoke-send alert-smoke-local live-smoke-preflight live-smoke network-smoke git-autosync-start git-autosync-stop git-autosync-status git-autosync-install git-autosync-uninstall

VENV_DIR := .venv
PYTHON := $(VENV_DIR)/bin/python
PIP := $(VENV_DIR)/bin/pip
MONITOR_MODE ?= both

venv:
	python3 -m venv $(VENV_DIR)

install:
	$(PIP) install -e .

env-check:
	$(PYTHON) scripts/check_env.py

test:
	./scripts/run_tests.sh

verify-stack:
	./scripts/verify_stack.sh

verify: env-check test start-stack verify-stack

network-smoke:
	$(PYTHON) scripts/network_smoke_test.py

one-click: network-smoke verify_one_click

one-click-lite:
	./scripts/verify_one_click.sh

verify_one_click:
	./scripts/verify_one_click.sh

desktop-command:
	./scripts/install_desktop_poly_command.sh

repair-desktop-launcher:
	./scripts/repair_desktop_poly_launcher.sh

replay:
	PYTHONPATH=src $(PYTHON) scripts/replay_runtime.py

replay-calibrate:
	PYTHONPATH=src $(PYTHON) scripts/replay_calibration.py

reconciliation-report:
	PYTHONPATH=src $(PYTHON) scripts/generate_reconciliation_report.py

full-validate:
	DRY_RUN=false PYTHONPATH=src $(PYTHON) scripts/full_flow_validate.py --bootstrap-stack

fault-drill:
	PYTHONPATH=src $(PYTHON) scripts/fault_drill.py

release-gate:
	PYTHONPATH=src $(PYTHON) scripts/release_gate_check.py

readiness-brief:
	PYTHONPATH=src $(PYTHON) scripts/readiness_brief.py

rehearsal-finalize:
	PYTHONPATH=src $(PYTHON) scripts/rehearsal_finalize.py

stop-stack:
	./scripts/stop_poly_stack.sh

monitor-12h:
	@STATE_PATH="$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py state_path)"; \
		BOT_LOG="$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py bot_log_path)"; \
		OUT="$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py monitor_12h_report_path)"; \
		INCONCLUSIVE="$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py monitor_12h_state_path)"; \
		JSON_OUT="$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py monitor_12h_json_path)"; \
		./scripts/monitor_thresholds_12h.sh "$$OUT" "$$BOT_LOG" "$${MON12H_WINDOW_SECONDS:-43200}" "$$INCONCLUSIVE" "$$STATE_PATH" "$$JSON_OUT"

monitor-30m:
	@STATE_PATH="$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py state_path)"; \
		BOT_LOG="$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py bot_log_path)"; \
		OUT="$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py monitor_30m_report_path)"; \
		INCONCLUSIVE="$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py monitor_30m_state_path)"; \
		JSON_OUT="$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py monitor_30m_json_path)"; \
		./scripts/monitor_thresholds_30m.sh "$$OUT" "$$BOT_LOG" "$${MON30M_WINDOW_SECONDS:-1800}" "$$INCONCLUSIVE" "$$STATE_PATH" "$$JSON_OUT"

monitor-reports:
	./scripts/run_monitor_reports.sh $(MONITOR_MODE)

stop-monitor-reports:
	./scripts/stop_monitor_reports.sh

monitor-scheduler-install:
	./scripts/install_monitor_scheduler.sh

monitor-scheduler-uninstall:
	./scripts/uninstall_monitor_scheduler.sh

monitor-scheduler-status:
	./scripts/monitor_scheduler_status.sh

monitor-scheduler-smoke:
	./scripts/verify_monitor_scheduler_nohup.sh

alert-smoke:
	DRY_RUN=false PYTHONPATH=src $(PYTHON) scripts/verify_alert_delivery.py

alert-smoke-send:
	DRY_RUN=false PYTHONPATH=src $(PYTHON) scripts/verify_alert_delivery.py --send-remote

alert-smoke-local:
	DRY_RUN=false PYTHONPATH=src $(PYTHON) scripts/verify_alert_delivery_local.py

live-smoke-preflight:
	DRY_RUN=false PYTHONPATH=src $(PYTHON) scripts/live_smoke_preflight.py

live-smoke:
	DRY_RUN=false ./scripts/run_live_smoke.sh "$${LIVE_SMOKE_TOKEN_ID:-}"

ops-12h:
	@echo "==> Pre-production ops: 12h rapid debrief template"
	@echo "File: preprod_operations_playbook.md -> section 七、12h 极简复盘（快速版）"
	@grep -n "## 七、12h 极简复盘（快速版）" -n preprod_operations_playbook.md
	@sed -n '/## 七、12h 极简复盘（快速版）/,/### 一句总结/p' preprod_operations_playbook.md

rehearse-10h:
	@echo "==> Start 10h paper rehearsal (10 checkpoints, 3600s interval)"
	@OUT="$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py rehearsal_10h_out_path)"; \
		LOG="$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py rehearsal_10h_log_path)"; \
		nohup bash ./scripts/rehearse_12h_paper.sh "$$OUT" 10 3600 > "$$LOG" 2>&1 & \
		echo "rehearsal_started=1"; \
		echo "result=$$OUT"; \
		echo "log=$$LOG"

rehearse-12h:
	@echo "==> rehearse-12h has been switched to 10h paper rehearsal"
	@$(MAKE) rehearse-10h

rehearse-24h:
	@echo "==> Start 24h paper rehearsal (24 checkpoints, 3600s interval)"
	@DEFAULT_OUT="$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py rehearsal_24h_out_path)"; \
		DEFAULT_LOG="$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py rehearsal_24h_log_path)"; \
		OUT="$${REHEARSE_24H_OUT:-$$DEFAULT_OUT}"; \
		LOG="$${REHEARSE_24H_LOG:-$$DEFAULT_LOG}"; \
		nohup bash ./scripts/rehearse_12h_paper.sh "$$OUT" "$${REHEARSE_24H_WINDOWS:-24}" "$${REHEARSE_24H_INTERVAL:-3600}" > "$$LOG" 2>&1 & \
		echo "rehearsal_started=1"; \
		echo "result=$$OUT"; \
		echo "log=$$LOG"

rehearse-24h-dry-run:
	./scripts/start_dry_run_rehearsal.sh "$${REHEARSE_24H_OUT:-}" "$${REHEARSE_24H_WINDOWS:-24}" "$${REHEARSE_24H_INTERVAL:-3600}"

rehearse-progress:
	@tail -n 20 "$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py rehearsal_10h_out_path)"

rehearse-24h-progress:
	@DEFAULT_OUT="$$(PYTHONPATH=src $(PYTHON) scripts/runtime_paths.py rehearsal_24h_dry_run_out_path)"; \
		tail -n 20 "$${REHEARSE_24H_OUT:-$$DEFAULT_OUT}"

run-once:
	$(VENV_DIR)/bin/polybot --once

run:
	$(VENV_DIR)/bin/polybot

start-stack:
	./scripts/start_poly_stack.sh

git-autosync-start:
	./scripts/start_git_autosync.sh

git-autosync-stop:
	./scripts/stop_git_autosync.sh

git-autosync-status:
	./scripts/git_autosync_status.sh

git-autosync-install:
	./scripts/install_git_autosync_launchd.sh

git-autosync-uninstall:
	./scripts/uninstall_git_autosync_launchd.sh
