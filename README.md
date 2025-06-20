# SQL Flow Analyzer

A tool to analyze SQL queries and generate data flow diagrams.

## Requirements

- Python 3.7+
- Graphviz (system dependency)

## Installation

1. Install Graphviz on your system:
   - Windows: Download from [Graphviz Downloads](https://graphviz.org/download/)
   - Linux: `sudo apt-get install graphviz`
   - macOS: `brew install graphviz`

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

## Usage

```bash
python -m sql_flow_analyzer analyze --sql-file path/to/your/queries.sql --output diagram.png
```

The tool will:
1. Parse the SQL queries
2. Extract table relationships and data flow
3. Generate a visual diagram showing how data moves between tables

## Example

The repository includes example SQL queries in the `examples` directory. Try them out:

```bash
python -m sql_flow_analyzer analyze --sql-file examples/sample_queries.sql --output flow.png
``` 