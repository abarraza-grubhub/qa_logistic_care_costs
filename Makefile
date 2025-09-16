.PHONY: help install install-dev test lint format type-check clean jupyter docs build upload
.DEFAULT_GOAL := help

PYTHON := python3
PIP := pip3
PACKAGE_NAME := qa_logistic_care_costs

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install package dependencies
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

install-dev: ## Install package with development dependencies
	$(PIP) install -r requirements.txt
	$(PIP) install -e ".[dev]"

test: ## Run tests
	pytest tests/ -v --cov=src/$(PACKAGE_NAME) --cov-report=html --cov-report=term

lint: ## Run code linting
	flake8 src/ tests/
	black --check src/ tests/

format: ## Format code
	black src/ tests/

type-check: ## Run type checking
	mypy src/$(PACKAGE_NAME)

clean: ## Clean up build artifacts and cache files
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

jupyter: ## Start Jupyter Lab
	jupyter lab notebooks/

docs: ## Generate documentation (placeholder)
	@echo "Documentation generation not yet implemented"

build: ## Build package
	$(PYTHON) setup.py sdist bdist_wheel

upload: ## Upload package to PyPI (placeholder)
	@echo "Package upload not yet configured"

setup-env: ## Set up development environment
	$(PYTHON) -m venv venv
	@echo "Virtual environment created. Activate with: source venv/bin/activate"

check: lint type-check test ## Run all checks (lint, type-check, test)

all: clean install-dev check ## Clean, install, and run all checks