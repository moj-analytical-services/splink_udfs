cmake_minimum_required(VERSION 3.5)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_subdirectory(third_party/rapidfuzz EXCLUDE_FROM_ALL)

set(TARGET_NAME splink_udfs)



set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

find_package(unofficial-utf8proc CONFIG REQUIRED)


include_directories(src/include)

set(EXTENSION_SOURCES src/splink_udfs_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})


target_link_libraries(${EXTENSION_NAME}           utf8proc rapidfuzz::rapidfuzz)
target_link_libraries(${LOADABLE_EXTENSION_NAME}  utf8proc rapidfuzz::rapidfuzz)

install(
  TARGETS ${EXTENSION_NAME} rapidfuzz
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
