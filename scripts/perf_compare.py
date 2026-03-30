#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parent.parent
TMP_DIR = ROOT / ".tmp" / "perf"
DATASET_PATH = TMP_DIR / "flat-100k.parquet"
GO_BIN = TMP_DIR / "go-parquet-tools"
PYTHON_CLI = ROOT / ".venv-perf" / "bin" / "parquet-tools"
REPORT_PATH = ROOT / "docs" / "performance-report.md"


@dataclass
class BenchmarkCase:
    name: str
    go_cmd: list[str]
    py_cmd: list[str]
    iterations: int = 5


def ensure_dirs() -> None:
    (ROOT / "docs").mkdir(exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)


def build_go_binary() -> None:
    env = os.environ.copy()
    env.setdefault("GOCACHE", str(ROOT / ".gocache"))
    env.setdefault("GOMODCACHE", str(ROOT / ".gomodcache"))
    subprocess.run(
        ["go", "build", "-o", str(GO_BIN), "."],
        cwd=ROOT,
        env=env,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def ensure_python_cli() -> None:
    if not PYTHON_CLI.exists():
        raise SystemExit(
            "Python parquet-tools is not installed. Expected local CLI at "
            f"{PYTHON_CLI}. Create .venv-perf and install parquet-tools first."
        )


def ensure_dataset() -> None:
    if DATASET_PATH.exists():
        return

    row_count = 100_000
    cities = ["Tokyo", "Shanghai", "Berlin", "New York", "Singapore"]
    payload = "payload-" * 8

    table = pa.table(
        {
            "id": list(range(1, row_count + 1)),
            "group": [i % 32 for i in range(row_count)],
            "name": [f"user-{i+1}" for i in range(row_count)],
            "city": [cities[i % len(cities)] for i in range(row_count)],
            "score": [((i * 37) % 1000) / 10.0 for i in range(row_count)],
            "active": [i % 3 == 0 for i in range(row_count)],
            "payload": [f"{payload}{i % 97}" for i in range(row_count)],
        }
    )
    pq.write_table(table, DATASET_PATH, compression="snappy")


def measure(cmd: list[str], env: dict[str, str] | None = None) -> float:
    started = time.perf_counter()
    subprocess.run(
        cmd,
        cwd=ROOT,
        env=env,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return time.perf_counter() - started


def run_case(case: BenchmarkCase) -> dict[str, object]:
    go_times = []
    py_times = []

    measure(case.go_cmd)
    measure(case.py_cmd)

    for _ in range(case.iterations):
        go_times.append(measure(case.go_cmd))
        py_times.append(measure(case.py_cmd))

    go_median = statistics.median(go_times)
    py_median = statistics.median(py_times)
    return {
        "name": case.name,
        "go_times": go_times,
        "py_times": py_times,
        "go_median": go_median,
        "py_median": py_median,
        "speedup": py_median / go_median if go_median else 0.0,
    }


def command_versions() -> dict[str, str]:
    return {
        "go": subprocess.check_output(["go", "version"], cwd=ROOT, text=True).strip(),
        "python": subprocess.check_output(
            [str(ROOT / ".venv-perf" / "bin" / "python"), "--version"],
            cwd=ROOT,
            text=True,
            stderr=subprocess.STDOUT,
        ).strip(),
        "python_parquet_tools": subprocess.check_output(
            [str(ROOT / ".venv-perf" / "bin" / "python"), "-m", "pip", "show", "parquet-tools"],
            cwd=ROOT,
            text=True,
        ).strip(),
    }


def format_seconds(value: float) -> str:
    return f"{value:.4f}s"


def render_report(results: list[dict[str, object]], versions: dict[str, str]) -> str:
    file_size = DATASET_PATH.stat().st_size
    package_version = "unknown"
    for line in versions["python_parquet_tools"].splitlines():
        if line.startswith("Version: "):
            package_version = line.split(": ", 1)[1]
            break

    lines = [
        "# Performance Report",
        "",
        "This report compares the Go CLI in this repository against the Python "
        "`parquet-tools` package on the overlapping commands: `show`, `csv`, and `inspect`.",
        "",
        "## Environment",
        "",
        f"- Date: {time.strftime('%Y-%m-%d')}",
        f"- Platform: {platform.platform()}",
        f"- Machine: {platform.machine()}",
        f"- {versions['go']}",
        f"- {versions['python']}",
        f"- python parquet-tools: {package_version}",
        "",
        "## Dataset",
        "",
        f"- Path: `{DATASET_PATH.relative_to(ROOT)}`",
        "- Rows: 100,000",
        "- Columns: 7",
        "- Schema: `id`, `group`, `name`, `city`, `score`, `active`, `payload`",
        f"- File size: {file_size / (1024 * 1024):.2f} MiB",
        "- Compression: snappy",
        "",
        "## Method",
        "",
        "- The Go binary is built once before benchmarking.",
        "- The Python CLI runs from a local virtual environment at `.venv-perf`.",
        "- Each command is warmed up once, then measured 5 times.",
        "- Reported latency is median wall-clock time.",
        "- Command output is discarded to focus on command execution time.",
        "",
        "## Results",
        "",
        "| Command | Go median | Python median | Go speedup |",
        "| --- | ---: | ---: | ---: |",
    ]

    for result in results:
        lines.append(
            f"| `{result['name']}` | {format_seconds(result['go_median'])} | "
            f"{format_seconds(result['py_median'])} | {result['speedup']:.2f}x |"
        )

    lines.extend(
        [
            "",
            "## Commands",
            "",
            "```bash",
            f"{GO_BIN.relative_to(ROOT)} show --limit 100 {DATASET_PATH.relative_to(ROOT)}",
            f".venv-perf/bin/parquet-tools show --head 100 {DATASET_PATH.relative_to(ROOT)}",
            f"{GO_BIN.relative_to(ROOT)} csv --limit 10000 {DATASET_PATH.relative_to(ROOT)}",
            f".venv-perf/bin/parquet-tools csv --head 10000 {DATASET_PATH.relative_to(ROOT)}",
            f"{GO_BIN.relative_to(ROOT)} inspect {DATASET_PATH.relative_to(ROOT)}",
            f".venv-perf/bin/parquet-tools inspect {DATASET_PATH.relative_to(ROOT)}",
            "```",
            "",
            "## Notes",
            "",
            "- This is an end-to-end CLI benchmark, not a library microbenchmark.",
            "- The comparison only covers commands shared by both implementations.",
            "- The current Go implementation is optimized for single-binary distribution, while the Python implementation relies on pandas and pyarrow.",
            "- Re-run this report with `python3 ./scripts/perf_compare.py` after changing I/O, rendering, or schema handling code.",
        ]
    )

    return "\n".join(lines) + "\n"


def main() -> int:
    ensure_dirs()
    ensure_python_cli()
    ensure_dataset()
    build_go_binary()

    results = []
    for case in [
        BenchmarkCase(
            name="show first 100 rows",
            go_cmd=[str(GO_BIN), "show", "--limit", "100", str(DATASET_PATH)],
            py_cmd=[str(PYTHON_CLI), "show", "--head", "100", str(DATASET_PATH)],
        ),
        BenchmarkCase(
            name="csv first 10000 rows",
            go_cmd=[str(GO_BIN), "csv", "--limit", "10000", str(DATASET_PATH)],
            py_cmd=[str(PYTHON_CLI), "csv", "--head", "10000", str(DATASET_PATH)],
        ),
        BenchmarkCase(
            name="inspect file metadata",
            go_cmd=[str(GO_BIN), "inspect", str(DATASET_PATH)],
            py_cmd=[str(PYTHON_CLI), "inspect", str(DATASET_PATH)],
        ),
    ]:
        results.append(run_case(case))

    report = render_report(results, command_versions())
    REPORT_PATH.write_text(report, encoding="utf-8")
    print(f"wrote {REPORT_PATH}")
    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
