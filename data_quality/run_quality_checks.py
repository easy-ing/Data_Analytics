# Great Expectations skeleton
# This script attempts to run a minimal set of data quality checks using Great Expectations.

import sys

try:
    import great_expectations as ge
    import pandas as pd
except Exception as e:
    print("Great Expectations not installed. Install with: pip install great_expectations")
    sys.exit(1)


def run_basic_checks(csv_path: str):
    df = pd.read_csv(csv_path)
    ge_df = ge.from_pandas(df)

    # 예제: id 컬럼이 null이 아니어야 함, event_type 컬럼은 특정 값 중 하나여야 함
    results = []
    results.append(ge_df.expect_column_values_to_not_be_null("id").success)
    results.append(ge_df.expect_column_values_to_be_in_set("event_type", ["view", "click", "like", "share"]).success)

    if all(results):
        print("Basic data quality checks passed")
        return 0
    else:
        print("Some checks failed")
        return 2


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python data_quality/run_quality_checks.py <path-to-csv>")
        sys.exit(1)
    sys.exit(run_basic_checks(sys.argv[1]))
