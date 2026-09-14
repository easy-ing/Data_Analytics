from __future__ import annotations
import json
import sys
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd
import great_expectations as ge


def load_expectations(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_checks(csv_path: str, suite_path: str, artifact_path: str | None = None) -> int:
    df = pd.read_csv(csv_path)
    ge_df = ge.from_pandas(df)

    suite = load_expectations(suite_path)
    expectations = suite.get("expectations", [])

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

            record.update({"success": bool(success), "result": details, "raw": res})
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

    with artifact_path.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("\nGE validation summary:")
    print(json.dumps(summary, indent=2))

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
                if res is not None:
                    # Try to print a concise part of result
                    try:
                        snippet = json.dumps(res)[:1000]
                    except Exception:
                        snippet = str(res)
                    print(f"  result: {snippet}")
                if shown >= 10:
                    print("  ... (more failures omitted)")
                    break

    artifact_rel = artifact_path.as_posix()
    print(f"\nValidation artifact written to: {artifact_rel}")

    # Exit code: 0 success, 1 failure
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
