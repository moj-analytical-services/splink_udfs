cmake_minimum_required(VERSION 3.12)
project(splink_udfs LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# DuckDB helper macros (defined by the extension toolchain)
add_extension_definitions()

# ---- vendored deps ----
add_subdirectory(third_party/rapidfuzz EXCLUDE_FROM_ALL)

# Make headers under src/include available
include_directories(src/include)

# ---- sources ----
set(EXTENSION_SOURCES
  src/splink_udfs_extension.cpp
  src/functions/trie/build_suffix_trie.cpp
  src/functions/trie/peel_end_tokens.cpp
  src/functions/address/build_cleaned_address.cpp
  src/functions/address/format_address_with_counts.cpp
  src/functions/address/format_address_with_term.cpp
  src/functions/address/find_address_from_trie.cpp
  src/common/trie/suffix_trie.cpp
  src/common/trie/trie_cache_utils.cpp
  src/common/trie/trie_nav.cpp
  src/common/trie/peel_utils.cpp
)

# ---- build targets ----
build_static_extension(splink_udfs ${EXTENSION_SOURCES})

# WASM escape hatch: bake static libs into the .wasm side-module
if(EMSCRIPTEN)
  set(DUCKDB_EXTENSION_SPLINK_UDFS_LINKED_LIBS
    $<TARGET_FILE:duckdb_utf8proc>
  )

endif()

# Loadable (.so/.dylib/.duckdb_extension.wasm)
build_loadable_extension(splink_udfs "-warnings" ${EXTENSION_SOURCES})

# ---- link for native builds (non-WASM) ----
# Use the plain signature to stay consistent with DuckDB's macros.
target_link_libraries(splink_udfs_extension          rapidfuzz::rapidfuzz duckdb_utf8proc)
target_link_libraries(splink_udfs_loadable_extension rapidfuzz::rapidfuzz duckdb_utf8proc)

# ---- install ----
# Also install/export the rapidfuzz target so the export set is complete.
# (The real library target name exported by RapidFuzz is usually 'rapidfuzz'.)
install(
  TARGETS splink_udfs_extension rapidfuzz
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
