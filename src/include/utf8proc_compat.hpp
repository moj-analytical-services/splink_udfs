// src/include/utf8proc_compat.hpp
#pragma once

#if __has_include(<utf8proc.h>)
// Upstream/vcpkg header: everything is in the global namespace.
#include <utf8proc.h>
#else
// DuckDB vendored header: typedefs are global, functions/enums are in duckdb::
#include <utf8proc.hpp>

using duckdb::utf8proc_errmsg;
using duckdb::utf8proc_iterate;
using duckdb::utf8proc_map;
using duckdb::utf8proc_option_t;

// Mirror the flags so callers can use the usual names
constexpr auto UTF8PROC_NULLTERM = duckdb::UTF8PROC_NULLTERM;
constexpr auto UTF8PROC_COMPAT = duckdb::UTF8PROC_COMPAT;
constexpr auto UTF8PROC_DECOMPOSE = duckdb::UTF8PROC_DECOMPOSE;
constexpr auto UTF8PROC_STRIPMARK = duckdb::UTF8PROC_STRIPMARK;
constexpr auto UTF8PROC_LUMP = duckdb::UTF8PROC_LUMP;
#endif