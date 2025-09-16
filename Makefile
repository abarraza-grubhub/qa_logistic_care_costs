.PHONY: install setup pre-commit help
.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

# --------------------------------------------------------------------
# RUN PRE-COMMIT HOOKS (LINTING, FORMATTING, ETC.)
# --------------------------------------------------------------------
pre-commit:
	pre-commit run --all-files -v

# --------------------------------------------------------------------
# SET UP ENVIRONMENT
# --------------------------------------------------------------------

install:
	uv pip install -r requirements.txt
	uv pip install pre-commit
	pre-commit install

setup: install