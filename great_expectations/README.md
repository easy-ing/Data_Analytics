Great Expectations (initialized)

This directory contains a minimal Great Expectations setup for the project:

- great_expectations.yml: minimal Data Context configuration using a PandasExecutionEngine and a filesystem data connector (data/raw)
- expectations/events_suite.json: example expectation suite for the events CSV
- checkpoints/ci_checkpoint.yml: simple checkpoint config pointing to data/raw/events_sample.csv
- scripts/run_ge_checks.py: lightweight runner to apply the expectation suite to a CSV file

Quick start:
1. Install dev dependencies
   pip install -r requirements-dev.txt
2. Generate sample CSV
   python etl/generate_dummy_data.py --output-dir data/raw
3. Run checks
   python scripts/run_ge_checks.py data/raw/events_sample.csv

To adopt full Great Expectations workflows:
- run `great_expectations init` locally to create a full GE directory structure
- use the GE CLI or DataContext API to create/manage expectation suites and checkpoints
