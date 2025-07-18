Below is a single, consolidated **“no-surprises” playbook** for adding any vcpkg-supplied library (utf8proc, fmt, simdjson …) to a DuckDB community extension that you generated from **`duckdb/extension-template`**.

It merges everything we learned in this conversation, fixes the subtle errors we hit along the way, and highlights the critical do’s & don’ts so the next integration is painless.

---

## 0 · Why this matters

* The template’s **Makefile → CMake → vcpkg** pipeline is opinionated: it assumes
  *manifest-mode* vcpkg, a **project-root `vcpkg.json`**, and a
  **tool-chain file supplied via `VCPKG_TOOLCHAIN_PATH`**.
* The DuckDB build then generates **two CMake targets** for your code
  (`<name>_extension` – static, and `<name>_loadable_extension` – shared).
  Linking the wrong thing to the wrong target = instant cycles.
* Some vcpkg ports use the **`unofficial-` prefix**; use the exact names vcpkg prints.

Nail those three rules and life is easy.

---

## 1 · 90-second Quick-Start (utf8proc example)

| Step                             | File / Command      | Minimal content                                                                                                                                                                                                                               |
| -------------------------------- | ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1.** Declare the dep           | **`vcpkg.json`**    | `json { "dependencies": [ "utf8proc" ] } `                                                                                                                                                                                                    |
| **2.** Install vcpkg (once)      | shell               | `bash git clone https://github.com/microsoft/vcpkg ~/dev/vcpkg && ~/dev/vcpkg/bootstrap-vcpkg.sh -disableMetrics export VCPKG_TOOLCHAIN_PATH=$HOME/dev/vcpkg/scripts/buildsystems/vcpkg.cmake `                                               |
| **3.** Edit **`CMakeLists.txt`** | (after `project()`) | `cmake find_package(unofficial-utf8proc CONFIG REQUIRED) build_static_extension(${TARGET_NAME} ${SRC}) build_loadable_extension(${TARGET_NAME} "" ${SRC}) target_link_libraries(${LOADABLE_EXTENSION_NAME} utf8proc)     # ← link ONLY here ` |
| **4.** Build                     | shell               | `bash rm -rf build  # whenever you change deps GEN=ninja make `                                                                                                                                                                               |
| **✓** Success message            | CMake output        | `-- Found unofficial-utf8proc: …/libutf8proc.a (found version "2.10.0")`                                                                                                                                                                      |

The resulting **`<ext>.duckdb_extension`** is the only file you ship.

---

## 2 · Deep Guide

### 2.1 How the template wires vcpkg

| Piece                            | Who sets it                  | Why it matters                                                                           |
| -------------------------------- | ---------------------------- | ---------------------------------------------------------------------------------------- |
| `VCPKG_TOOLCHAIN_PATH`           | **You** (env var)            | Tells CMake to load vcpkg.                                                               |
| `VCPKG_MANIFEST_DIR`             | **Makefile** (`${PROJ_DIR}`) | Points vcpkg at `./vcpkg.json`; enables *manifest mode*.                                 |
| Triplet (`VCPKG_TARGET_TRIPLET`) | **CI** or you                | `arm64-osx`, `x64-osx`, `x64-linux`, `x64-windows-static-md` … must match the host arch. |
| Auto-install                     | vcpkg + CMake                | On first configure, vcpkg runs `install` for anything in the manifest.                   |

### 2.2 CMake ordering rules

```cmake
project(my_ext)            # 1️⃣ activates toolchain
find_package(...)          # 2️⃣ locate deps
build_static_extension()   # 3️⃣ define targets
build_loadable_extension()
target_link_libraries(<loadable> …)   # 4️⃣ link ONLY the shared lib
```

*Swapping 1 ↔ 2 fails (toolchain not active).
Linking the **static** target introduces a duckdb\_static ↔ extension cycle.*

### 2.3 Naming conventions

| Scenario                                                                          | `find_package()` argument             | Imported target(s)                                                                    |
| --------------------------------------------------------------------------------- | ------------------------------------- | ------------------------------------------------------------------------------------- |
| Port ships its own config (e.g. **fmt**, **simdjson**)                            | `fmt CONFIG REQUIRED`                 | `fmt::fmt`                                                                            |
| Port supplies an *unofficial* config (e.g. **utf8proc**, **brotli**, **sqlite3**) | `unofficial-utf8proc CONFIG REQUIRED` | `utf8proc`<br>(sometimes `unofficial::<pkg>::<target>` – check vcpkg “usage” message) |

> **Tip:** After any `vcpkg install …`, vcpkg prints a **Usage** block that shows
> the exact `find_package`/target names – copy them verbatim.

### 2.4 Triplets & CRT sanity (Windows note)

* DuckDB CI uses **`x64-windows-static-md`** (static libs, **dynamic** MSVC runtime `/MD`).
* Set the same triplet locally:
  `export VCPKG_TARGET_TRIPLET=x64-windows-static-md`.

### 2.5 Macros cheat-sheet

| Macro                        | Creates                                                      | Links to         | When to link extra libs            |
| ---------------------------- | ------------------------------------------------------------ | ---------------- | ---------------------------------- |
| `build_static_extension()`   | `<name>_extension` (`STATIC`)                                | `duckdb_static`  | **Never** (avoid cycles)           |
| `build_loadable_extension()` | `<name>_loadable_extension` (`SHARED` → `.duckdb_extension`) | *none* initially | **Always** (external libs go here) |

---

## 3 · Do / Don’t crib sheet

| ✅ Do                                                            | ❌ Don’t                                                 |
| --------------------------------------------------------------- | ------------------------------------------------------- |
| Export `VCPKG_TOOLCHAIN_PATH` **before** running `make`.        | Edit the template Makefile (you’ll break CI).           |
| Use `find_package(unofficial-<pkg> CONFIG)` when vcpkg says so. | Assume the port name equals the CMake package name.     |
| Link **only** the loadable target to external libs.             | Add `target_link_libraries` to the static extension.    |
| Wipe `build/` after changing triplets or deps.                  | Keep stale CMake caches – they hide wrong triplets.     |
| Treat `utf8proc_option_t` (not `int`) when OR-ing flags.        | Cast later – compile will fail with signature mismatch. |

---

## 4 · Canonical example (utf8proc)

```jsonc
// vcpkg.json
{ "dependencies": [ "utf8proc" ] }
```

```cmake
cmake_minimum_required(VERSION 3.5)
project(text_ext CXX)

find_package(unofficial-utf8proc CONFIG REQUIRED)

set(SRC src/text_ext.cpp)
build_static_extension(text ${SRC})
build_loadable_extension(text "" ${SRC})
target_link_libraries(text_loadable_extension utf8proc)
```

C++ wrapper:

```cpp
#include <utf8proc.h>
#include <memory>
#include <string>

namespace util {
struct Free { void operator()(utf8proc_uint8_t* p) const { free(p);} };
std::string strip(const std::string& s) {
    utf8proc_uint8_t* tmp = nullptr;
    constexpr utf8proc_option_t F =
        UTF8PROC_NULLTERM | UTF8PROC_DECOMPOSE |
        UTF8PROC_COMPAT   | UTF8PROC_STRIPMARK | UTF8PROC_LUMP;
    if (utf8proc_map(reinterpret_cast<const utf8proc_uint8_t*>(s.c_str()),
                     0, &tmp, F) < 0)
        throw std::runtime_error("utf8proc_map failed");
    std::unique_ptr<utf8proc_uint8_t,Free> holder(tmp);
    return std::string(reinterpret_cast<char*>(holder.get()));
}}
```

Build:

```bash
export VCPKG_TOOLCHAIN_PATH=$HOME/dev/vcpkg/scripts/buildsystems/vcpkg.cmake
export VCPKG_TARGET_TRIPLET=arm64-osx   # or x64-linux / x64-windows-static-md
GEN=ninja make
```

Success log excerpt:

```
-- Detecting compiler hash for triplet arm64-osx…
-- Found unofficial-utf8proc: …/libutf8proc.a (found version "2.10.0")
[100%] Built target text_loadable_extension
```

Load test:

```sql
.load build/release/extension/text/text.duckdb_extension;
SELECT util.strip('Jürgen – Ærøskøbing') AS folded;  -- ➜ Jurgen – Aroskobing
```

---

### Keep this sheet handy

Follow the order, respect the naming, link only the loadable target, and adding the next dependency will be a **copy-paste-and-go** affair rather than a debugging marathon.
