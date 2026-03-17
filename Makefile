.PHONY: venv install env-check test verify-stack verify one-click verify_one_click run-once run start-stack replay replay-calibrate stop-stack ops-12h rehearse-10h rehearse-12h rehearse-progress monitor-12h monitor-30m monitor-reports stop-monitor-reports monitor-scheduler-install monitor-scheduler-uninstall monitor-scheduler-status network-smoke

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

verify: env-check test verify-stack

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

stop-stack:
	./scripts/stop_poly_stack.sh

monitor-12h:
	./scripts/monitor_thresholds_12h.sh

monitor-30m:
	./scripts/monitor_thresholds_30m.sh

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

ops-12h:
	@echo "==> Pre-production ops: 12h rapid debrief template"
	@echo "File: preprod_operations_playbook.md -> section 七、12h 极简复盘（快速版）"
	@grep -n "## 七、12h 极简复盘（快速版）" -n preprod_operations_playbook.md
	@sed -n '/## 七、12h 极简复盘（快速版）/,/### 一句总结/p' preprod_operations_playbook.md

rehearse-10h:
	@echo "==> Start 10h paper rehearsal (10 checkpoints, 3600s interval)"
	@nohup bash ./scripts/rehearse_12h_paper.sh /tmp/poly_10h_paper_rehearsal.txt 10 3600 > /tmp/poly_10h_paper_rehearsal.log 2>&1 &
	@echo "rehearsal_started=1"
	@echo "result=/tmp/poly_10h_paper_rehearsal.txt"
	@echo "log=/tmp/poly_10h_paper_rehearsal.log"

rehearse-12h:
	@echo "==> rehearse-12h has been switched to 10h paper rehearsal"
	@$(MAKE) rehearse-10h

rehearse-progress:
	@tail -n 20 /tmp/poly_10h_paper_rehearsal.txt

run-once:
	$(VENV_DIR)/bin/polybot --once

run:
	$(VENV_DIR)/bin/polybot

start-stack:
	./scripts/start_poly_stack.sh
