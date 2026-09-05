from __future__ import annotations
import json
import sys
import pandas as pd
import great_expectations as ge


def load_expectations(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def run_checks(csv_path: str, suite_path: str) -> int:
    df = pd.read_csv(csv_path)
    ge_df = ge.from_pandas(df)

    suite = load_expectations(suite_path)
    expectations = suite.get('expectations', [])

    results = []
    for exp in expectations:
        exp_type = exp.get('expectation_type')
        kwargs = exp.get('kwargs', {})
        if not hasattr(ge_df, exp_type):
            print(f"Skipping unknown expectation: {exp_type}")
            continue
        method = getattr(ge_df, exp_type)
        try:
            res = method(**kwargs)
            success = res.get('success') if isinstance(res, dict) else bool(res)
        except Exception as e:
            print(f"Expectation {exp_type} raised exception: {e}")
            success = False
        results.append((exp_type, success))
        print(f"{exp_type} -> {'PASS' if success else 'FAIL'}")

    all_ok = all(r[1] for r in results)
    if all_ok:
        print('\nAll checks passed')
        return 0
    else:
        print('\nOne or more checks failed')
        return 2


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python scripts/run_ge_checks.py <path-to-csv> [<path-to-suite>]', file=sys.stderr)
        sys.exit(1)
    csv_path = sys.argv[1]
    suite_path = sys.argv[2] if len(sys.argv) > 2 else 'great_expectations/expectations/events_suite.json'
    sys.exit(run_checks(csv_path, suite_path))
