from __future__ import annotations
import json
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import great_expectations as ge


def load_expectations(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _serialize(obj: Any) -> Any:
    # Fallback serializer for numpy / pandas types
    try:
        return json.loads(json.dumps(obj))
    except Exception:
        return str(obj)


def _extract_unexpected_samples(res: dict, df: pd.DataFrame, max_samples: int = 5) -> list:
    """Try to extract sample unexpected rows/values from a GE result dict.

    Returns a list of row dicts or value samples (up to max_samples).
    """
    samples = []

    result = res.get("result") if isinstance(res, dict) else None
    if not isinstance(result, dict):
        return samples

    # common keys in GE result for unexpected items
    # 1) unexpected_list (values)
    unexpected_list = result.get("unexpected_list") or result.get("partial_unexpected_list")
    if unexpected_list:
        # If we have full row values or scalar values, return first N
        for v in unexpected_list[:max_samples]:
            samples.append(_serialize(v))
        return samples

    # 2) unexpected_index_list (row indices)
    unexpected_idx = result.get("unexpected_index_list") or result.get("partial_unexpected_index_list")
    if unexpected_idx and isinstance(unexpected_idx, (list, tuple)) and len(unexpected_idx) > 0:
        # Clip to available indices
        for i in unexpected_idx[:max_samples]:
            try:
                # If index is out-of-range for pandas positional, try iloc
                row = df.iloc[int(i)].to_dict()
            except Exception:
                # Try label-based access
                try:
                    row = df.loc[i].to_dict()
                except Exception:
                    row = {"index": i}
            samples.append({k: _serialize(v) for k, v in row.items()})
        return samples

    # 3) unexpected_count and maybe unexpected_values
    unexpected_values = result.get("unexpected_values")
    if unexpected_values:
        for v in unexpected_values[:max_samples]:
            samples.append(_serialize(v))
        return samples

    return samples


def run_checks(csv_path: str, suite_path: str, artifact_path: str | None = None, max_samples: int = 5) -> int:
    df = pd.read_csv(csv_path)
    ge_df = ge.from_pandas(df)

    suite = load_expectations(suite_path)
    expectations = suite.get("expectations", [])

    # dataset metadata
    dataset_meta = {
        "row_count": len(df),
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
    }

    results = []
    for idx, exp in enumerate(expectations, start=1):
        exp_type = exp.get("expectation_type")
        kwargs = exp.get("kwargs", {})

        record: dict = {"expectation_type": exp_type, "kwargs": kwargs, "index": idx}

        if not hasattr(ge_df, exp_type):
            msg = f"Unknown expectation: {exp_type} (skipped)"
            print(msg)
            record.update({"skipped": True, "success": None, "error": msg})
            results.append(record)
            continue

        method = getattr(ge_df, exp_type)
        try:
            res = method(**kwargs)
            # GE expectation result is typically a dict-like object
            if isinstance(res, dict):
                success = res.get("success")
                details = res.get("result")
            else:
                # Fallback: try to interpret truthiness
                success = bool(res)
                details = None

            record.update({"success": bool(success), "result": details, "raw": _serialize(res)})

            # extract unexpected samples if present
            try:
                samples = _extract_unexpected_samples(res, df, max_samples=max_samples)
                if samples:
                    record["unexpected_samples"] = samples
            except Exception as e:
                record.setdefault("notes", []).append(f"failed_sample_extraction: {e}")

            print(f"{exp_type} -> {'PASS' if success else 'FAIL'}")
        except Exception as e:
            tb = traceback.format_exc()
            print(f"Expectation {exp_type} raised exception: {e}\n{tb}")
            record.update({"success": False, "error": str(e), "traceback": tb})
        results.append(record)

    passed = sum(1 for r in results if r.get("success") is True)
    failed = sum(1 for r in results if r.get("success") is False)
    skipped = sum(1 for r in results if r.get("skipped"))

    summary = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "csv_path": csv_path,
        "suite_path": suite_path,
        "dataset": dataset_meta,
        "counts": {"total": len(results), "passed": passed, "failed": failed, "skipped": skipped},
    }

    output = {"summary": summary, "results": results}

    # Write artifact
    if artifact_path is None:
        artifact_dir = Path("build")
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = artifact_dir / f"ge_validation_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
    else:
        artifact_path = Path(artifact_path)
        artifact_path.parent.mkdir(parents=True, exist_ok=True)

    # Use default=str to help serialize pandas/numpy types
    with artifact_path.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)

    print("\nGE validation summary:")
    print(json.dumps(summary, indent=2, default=str))

    if failed > 0:
        print("\nFailed expectations (up to 10 shown):")
        shown = 0
        for r in results:
            if r.get("success") is False:
                shown += 1
                etype = r.get("expectation_type")
                kws = r.get("kwargs")
                err = r.get("error")
                res = r.get("result")
                print(f"- {etype} kwargs={kws}")
                if err:
                    print(f"  error: {err}")
                if r.get("unexpected_samples"):
                    print(f"  unexpected_samples (up to {max_samples}):")
                    for s in r.get("unexpected_samples"):
                        print(f"    - {s}")
                elif res is not None:
                    # Try to print a concise part of result
                    try:
                        snippet = json.dumps(res, default=str)[:1000]
                    except Exception:
                        snippet = str(res)
                    print(f"  result: {snippet}")
                if shown >= 10:
                    print("  ... (more failures omitted)")
                    break

    artifact_rel = artifact_path.as_posix()
    print(f"\nValidation artifact written to: {artifact_rel}")

    # Exit code: 0 success, 1 failure, 2 misuse
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/run_ge_checks.py <path-to-csv> [<path-to-suite>] [<artifact-path>]", file=sys.stderr)
        sys.exit(2)
    csv_path = sys.argv[1]
    suite_path = sys.argv[2] if len(sys.argv) > 2 else "great_expectations/expectations/events_suite.json"
    artifact_path = sys.argv[3] if len(sys.argv) > 3 else None
    rc = run_checks(csv_path, suite_path, artifact_path)
    sys.exit(rc)
