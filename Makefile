SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

TF      := terraform -chdir=terraform
VENV    := .venv
PY      := $(VENV)/bin/python
CLIENTS := clients
N       ?= 5

# Load Confluent Cloud credentials from .env (if present) for every target.
ifneq (,$(wildcard .env))
include .env
export
endif

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ----------------------------------------------------------------- Terraform

.PHONY: init
init: ## terraform init
	$(TF) init -input=false

.PHONY: plan
plan: init ## terraform plan (writes terraform/tfplan)
	$(TF) plan -input=false -out=tfplan

.PHONY: apply
apply: ## Apply the reviewed plan from `make plan`
	$(TF) apply -input=false tfplan
	@rm -f terraform/tfplan
	@$(MAKE) --no-print-directory config

.PHONY: config
config: ## Write clients/config.json from terraform outputs (contains secrets, git-ignored)
	@$(TF) output -json client_config > $(CLIENTS)/config.json
	@chmod 600 $(CLIENTS)/config.json
	@echo "wrote $(CLIENTS)/config.json"

.PHONY: destroy
destroy: ## Tear everything down
	$(TF) destroy -input=false
	@rm -f $(CLIENTS)/config.json

# ------------------------------------------------------------------- Clients

STAMP := $(VENV)/.installed

$(STAMP): $(CLIENTS)/requirements.txt
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q --upgrade pip
	$(VENV)/bin/pip install -q -r $(CLIENTS)/requirements.txt
	@touch $(STAMP)

.PHONY: venv
venv: $(STAMP) ## Create the Python virtualenv

.PHONY: csfle-produce csfle-consume csfle-intruder
csfle-produce: $(STAMP) ## CSFLE: produce N customers (PII fields encrypted)
	$(PY) $(CLIENTS)/demo.py csfle produce -n $(N)
csfle-consume: $(STAMP) ## CSFLE: consume WITH KMS access (plaintext)
	$(PY) $(CLIENTS)/demo.py csfle consume -n $(N)
csfle-intruder: $(STAMP) ## CSFLE: consume WITHOUT KMS access (PII stays ciphertext)
	$(PY) $(CLIENTS)/demo.py csfle consume -n $(N) --no-kms

.PHONY: cspe-produce cspe-consume cspe-intruder
cspe-produce: $(STAMP) ## CSPE: produce N payments (whole payload encrypted)
	$(PY) $(CLIENTS)/demo.py cspe produce -n $(N)
cspe-consume: $(STAMP) ## CSPE: consume WITH KMS access (plaintext)
	$(PY) $(CLIENTS)/demo.py cspe consume -n $(N)
cspe-intruder: $(STAMP) ## CSPE: consume WITHOUT KMS access (nothing readable)
	$(PY) $(CLIENTS)/demo.py cspe consume -n $(N) --no-kms

.PHONY: test
test: $(STAMP) ## End-to-end test of both demos against the deployed stack
	$(PY) $(CLIENTS)/e2e_test.py
