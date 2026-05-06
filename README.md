# Gaia Integration Tool

A tool for integrating Gaia data using Dagster assets.

## Installation

Clone the repository, create a virtual environment, and install in editable mode:

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e .
```

## Usage

Run the Dagster development server:

```bash
dagster dev
```

This will start the Dagster UI where you can view and execute assets.

For the CLI tool:

```bash
gaia-tool
```

Or directly:

```bash
python -m gaia_integration_tool.main
```

## Development

To run tests:

```bash
python -m unittest discover tests
```

## Dagster Assets

The project defines Dagster assets for data processing:

- `gaia_data_asset`: Loads and processes Gaia data.
- `integrated_data`: Depends on `gaia_data_asset` and performs integration.