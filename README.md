<<<<<<< HEAD
# qa_logistic_care_costs
=======
# QA Logistic Care Costs

Analytics and modeling for QA logistic care costs at Grubhub.

## Project Structure

```
qa_logistic_care_costs/
├── src/                          # Source code (empty, ready for development)
├── notebooks/                    # Jupyter notebooks (empty, ready for analysis)
├── tests/                        # Unit tests
├── docs/                         # Documentation
├── requirements.txt              # Core Python dependencies
├── dev-requirements.txt          # Development dependencies
├── .pre-commit-config.yaml       # Pre-commit hooks configuration
├── pyproject.toml               # Tool configurations
├── Makefile                     # Development automation
└── README.md                    # This file
```

## Quick Start

### 1. Set up development environment

```bash
# Create and activate virtual environment
make setup-env
source venv/bin/activate

# Install dependencies
make install-dev
```

### 2. Set up pre-commit hooks (optional)

```bash
make setup-pre-commit
```

### 3. Start Jupyter Lab for analysis

```bash
make jupyter
```

### 4. Run code quality checks

```bash
make check  # Runs linting and tests
```

## Development Commands

The project includes a Makefile with common development tasks:

- `make help` - Show available commands
- `make install` - Install package dependencies
- `make install-dev` - Install with development dependencies
- `make test` - Run tests with coverage
- `make lint` - Check code style
- `make format` - Format code with Black
- `make clean` - Remove build artifacts
- `make jupyter` - Start Jupyter Lab
- `make setup-pre-commit` - Set up pre-commit hooks
- `make check` - Run all quality checks

## Contributing

1. Create a new branch for your feature
2. Make your changes
3. Run quality checks: `make check`
4. Submit a pull request

## Getting Started

This repository provides a minimal foundation for data science work. The `src/` and `notebooks/` directories are empty and ready for your code and analysis.
>>>>>>> 8221de7 (Address PR review feedback: simplify structure and remove bloat)
