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




## Tidy check
```
uv run duckdb/scripts/run-clang-tidy.py \
  "$PWD/src/.*/" \
  -header-filter "$PWD/src/.*/" \
  -p build/tidy \
  -j 8 \
  -fix -format
  ```

**DuckDB extension C++ — keep CI happy**

1. **Always use braces** around every `if/else/for/while/do` body. No single-line statements.
2. **No C-style casts.** Use `static_cast`/`dynamic_cast`/`const_cast`/`reinterpret_cast`. For downcasts: check type then `static_cast`, or just `dynamic_cast`.
3. **Pass heavy things by const ref.** Prefer `const T&` for `std::string`, vectors, smart pointers, etc.
4. **unique\_ptr returns:** returning a local `std::unique_ptr` → `return std::move(p);`.
5. **Headers:** `#pragma once`, include exactly what you use, project-relative includes (e.g. `"trie/suffix_trie.hpp"`), **no** `using namespace` in headers.
6. **Const & nullptr:** be const-correct; use `nullptr`; cast explicitly to avoid narrowing.
7. **DuckDB vector API pattern:** `ToUnifiedFormat` → check `validity` → write with `StringVector::AddString` → for lists set sizes via `ListVector::SetListSize`.
8. **RAII + ownership:** prefer `make_unique` and containers; if you `new` (e.g., agg state), ensure a matching destructor.

Stick to those and you’ll satisfy the clang-tidy rules that bit you (braces, no C-casts, no unnecessary by-value) and avoid the MinGW `unique_ptr` return quirk.
