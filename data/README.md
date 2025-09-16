# Data Directory

This directory contains data files for the project.

## Organization

- **raw/**: Original, immutable data dump
- **processed/**: Cleaned and processed data
- **external/**: External data sources
- **interim/**: Intermediate data that has been transformed

## Usage

Place data files in appropriate subdirectories. Use version control for small, essential datasets, but avoid committing large data files.

For large datasets, consider using:
- Git LFS (Large File Storage)
- Cloud storage with appropriate access controls
- Data versioning tools like DVC