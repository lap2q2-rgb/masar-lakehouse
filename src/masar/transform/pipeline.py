import os
import subprocess
import sys
import time

from masar.config import LAKEHOUSE, ROOT
from masar.quality.contract import INTEGRITY, load, validate
from masar.transform.build_gold import (build_trip_features,
                                        build_zone_hourly_demand,
                                        optimize_and_vacuum)


def _sh(*cmd, cwd=ROOT):
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "MASAR_HOME": str(ROOT)}
    subprocess.run(cmd, cwd=cwd, env=env, check=True)


def land_bronze():
    if (LAKEHOUSE / "bronze" / "trips" / "_delta_log").exists():
        print("  bronze already landed: skipping")
        return
    _sh(sys.executable, "-m", "masar.ingest.land_bronze")


def register_bronze():
    _sh(sys.executable, "-m", "masar.register")


def build_silver():
    _sh("dbt", "run", "--select", "silver_trips", cwd=ROOT / "dbt" / "masar")


def quality_check():
    results = validate(load("silver"))
    for name, r in results.items():
        print(f"  {name:18} passed={r['passed']!s:5} bad_rows={r['bad_rows']}")
    broken = [n for n in INTEGRITY if not results[n]["passed"]]
    if broken:
        raise RuntimeError(f"FAIL-FAST: integrity checks failed {broken}")
    failed = [n for n, r in results.items() if not r["passed"]]
    if failed:
        raise RuntimeError(f"quality checks failed: {failed}")


TASKS = [  # (name, function, depends_on)
    ("land_bronze",        land_bronze,              []),
    ("register_bronze",    register_bronze,          ["land_bronze"]),
    ("build_silver",       build_silver,             ["register_bronze"]),
    ("quality_check",      quality_check,            ["build_silver"]),
    ("gold_zone_demand",   build_zone_hourly_demand, ["quality_check"]),
    ("gold_trip_features", build_trip_features,      ["quality_check"]),
    ("optimize_vacuum",    optimize_and_vacuum,      ["gold_zone_demand", "gold_trip_features"]),
]


def run(retries: int = 2) -> None:
    done = set()
    for name, fn, deps in TASKS:
        assert set(deps) <= done, f"{name} would run before {deps}"
        for attempt in range(retries + 1):
            try:
                if os.environ.get("MASAR_FAIL_TASK") == name:
                    raise RuntimeError("injected failure")
                fn()
                print(f"[{name}] OK")
                done.add(name)
                break
            except Exception as e:
                print(f"[{name}] attempt {attempt + 1} failed: {e}")
                if attempt == retries:
                    print("PIPELINE HALTED")
                    sys.exit(1)
                time.sleep(2)


if __name__ == "__main__":
    run()
