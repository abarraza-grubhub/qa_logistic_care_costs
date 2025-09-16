# QA Logistic Care Costs

Analytics and modeling for QA logistic care costs at Grubhub.

## Project Structure

```
qa_logistic_care_costs/
├── src/
│   └── qa_logistic_care_costs/     # Main Python package
│       ├── __init__.py
│       ├── data_processing.py      # Data loading and cleaning utilities
│       └── analysis.py             # Analysis and visualization tools
├── notebooks/                      # Jupyter notebooks
│   ├── exploratory/               # Exploratory data analysis
│   ├── modeling/                  # Model development
│   └── reports/                   # Final analysis reports
├── tests/                         # Unit tests
├── data/                          # Data files (not committed to git)
│   ├── raw/                       # Original data
│   ├── processed/                 # Cleaned data
│   ├── external/                  # External data sources
│   └── interim/                   # Intermediate data
├── docs/                          # Documentation
├── requirements.txt               # Python dependencies
├── setup.py                       # Package installation configuration
├── Makefile                       # Development automation
└── README.md                      # This file
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

### 2. Run tests to verify setup

```bash
make test
```

### 3. Start Jupyter Lab for analysis

```bash
make jupyter
```

### 4. Run code quality checks

```bash
make check  # Runs linting, type checking, and tests
```

## Development Commands

The project includes a Makefile with common development tasks:

- `make help` - Show available commands
- `make install` - Install package dependencies
- `make install-dev` - Install with development dependencies
- `make test` - Run tests with coverage
- `make lint` - Check code style
- `make format` - Format code with Black
- `make type-check` - Run type checking with mypy
- `make clean` - Remove build artifacts
- `make jupyter` - Start Jupyter Lab
- `make check` - Run all quality checks
- `make build` - Build package for distribution

## Package Usage

```python
from qa_logistic_care_costs import data_processing, analysis

# Load and clean data
df = data_processing.load_data('data/raw/costs.csv')
cleaned_df = data_processing.clean_data(df)

# Perform analysis
stats = analysis.descriptive_stats(cleaned_df)
correlation_matrix = analysis.correlation_analysis(cleaned_df)
```

## Contributing

1. Create a new branch for your feature
2. Make your changes
3. Run quality checks: `make check`
4. Submit a pull request

## Data Organization

- Place raw data in `data/raw/`
- Store processed data in `data/processed/`
- Use `data/external/` for third-party data sources
- Keep intermediate results in `data/interim/`

Note: Data files are not committed to version control. Use appropriate data management practices for sensitive information.