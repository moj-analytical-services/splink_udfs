# Code Formatting Guide

This repository follows DuckDB’s standard format script so that C++, Python, CMake and SQLLogicTest files all look the same.

## Quick fix

```bash
# run from the repository root
uv run duckdb/scripts/format.py --all --fix --directories src test
```

## CI‑equivalent check

```bash
uv run duckdb/scripts/format.py --all --check --directories src test
```

CI runs exactly this `--all --check` command. Run it locally before pushing so you don’t get surprised.

## Required tools

* **clang‑format 11.0.1**
* **black ≥ 24**
* **cmake‑format**

