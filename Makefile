.PHONY: venv install env-check run-once run start-stack

VENV_DIR := .venv
PYTHON := $(VENV_DIR)/bin/python
PIP := $(VENV_DIR)/bin/pip

venv:
	python3 -m venv $(VENV_DIR)

install:
	$(PIP) install -e .

env-check:
	$(PYTHON) scripts/check_env.py

run-once:
	$(VENV_DIR)/bin/polybot --once

run:
	$(VENV_DIR)/bin/polybot

start-stack:
	./scripts/start_poly_stack.sh
