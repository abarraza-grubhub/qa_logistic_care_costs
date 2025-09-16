.PHONY: help install install-dev test lint format clean jupyter setup-env check
.DEFAULT_GOAL := help

PYTHON := python3
PIP := pip3

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install package dependencies
	$(PIP) install -r requirements.txt

install-dev: ## Install package with development dependencies
	$(PIP) install -r requirements.txt
	$(PIP) install -r dev-requirements.txt

test: ## Run tests
	pytest tests/ -v --cov-report=html --cov-report=term

lint: ## Run code linting
	black --check src/ tests/ notebooks/

format: ## Format code
	black src/ tests/ notebooks/

clean: ## Clean up build artifacts and cache files
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

jupyter: ## Start Jupyter Lab
	jupyter lab notebooks/

setup-env: ## Set up development environment
	$(PYTHON) -m venv venv
	@echo "Virtual environment created. Activate with: source venv/bin/activate"

setup-pre-commit: ## Set up pre-commit hooks
	pre-commit install

check: lint test ## Run all checks (lint, test)