"""Small native DuckDB C API helper for nickname version ownership tests."""

import ctypes as c
import time


class Result(c.Structure):
    _fields_ = [
        ("column_count", c.c_uint64),
        ("row_count", c.c_uint64),
        ("rows_changed", c.c_uint64),
        ("columns", c.c_void_p),
        ("error", c.c_void_p),
        ("internal", c.c_void_p),
    ]


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


class Database:
    def __init__(self, library):
        self.lib = c.CDLL(str(library))
        signatures = {
            "duckdb_create_config": (c.c_int, [c.POINTER(c.c_void_p)]),
            "duckdb_set_config": (c.c_int, [c.c_void_p, c.c_char_p, c.c_char_p]),
            "duckdb_destroy_config": (None, [c.POINTER(c.c_void_p)]),
            "duckdb_open_ext": (c.c_int, [c.c_char_p, c.POINTER(c.c_void_p), c.c_void_p, c.POINTER(c.c_void_p)]),
            "duckdb_connect": (c.c_int, [c.c_void_p, c.POINTER(c.c_void_p)]),
            "duckdb_disconnect": (None, [c.POINTER(c.c_void_p)]),
            "duckdb_close": (None, [c.POINTER(c.c_void_p)]),
            "duckdb_query": (c.c_int, [c.c_void_p, c.c_char_p, c.POINTER(Result)]),
            "duckdb_result_error": (c.c_char_p, [c.POINTER(Result)]),
            "duckdb_destroy_result": (None, [c.POINTER(Result)]),
            "duckdb_column_count": (c.c_uint64, [c.POINTER(Result)]),
            "duckdb_row_count": (c.c_uint64, [c.POINTER(Result)]),
            "duckdb_value_varchar": (c.c_void_p, [c.POINTER(Result), c.c_uint64, c.c_uint64]),
            "duckdb_free": (None, [c.c_void_p]),
        }
        for name, (result, arguments) in signatures.items():
            function = getattr(self.lib, name)
            function.restype, function.argtypes = result, arguments
        config = c.c_void_p()
        self.db, self.connection = c.c_void_p(), c.c_void_p()
        error = c.c_void_p()
        if self.lib.duckdb_create_config(c.byref(config)):
            raise RuntimeError("Cannot create DuckDB configuration")
        try:
            if self.lib.duckdb_set_config(config, b"allow_unsigned_extensions", b"true"):
                raise RuntimeError("Cannot enable loading the local extension")
            if self.lib.duckdb_open_ext(None, c.byref(self.db), config, c.byref(error)):
                message = c.string_at(error).decode() if error else "Cannot open DuckDB"
                self.lib.duckdb_free(error)
                raise RuntimeError(message)
        finally:
            self.lib.duckdb_destroy_config(c.byref(config))
        if self.lib.duckdb_connect(self.db, c.byref(self.connection)):
            raise RuntimeError("Cannot connect to DuckDB")

    def query(self, sql, fetch=False):
        result = Result()
        start = time.perf_counter()
        try:
            if self.lib.duckdb_query(self.connection, sql.encode(), c.byref(result)):
                raise RuntimeError(self.lib.duckdb_result_error(c.byref(result)).decode())
            rows = self.lib.duckdb_row_count(c.byref(result))
            values = []
            if fetch:
                for row in range(rows):
                    record = []
                    for column in range(self.lib.duckdb_column_count(c.byref(result))):
                        value = self.lib.duckdb_value_varchar(c.byref(result), column, row)
                        record.append(c.string_at(value).decode() if value else None)
                        self.lib.duckdb_free(value)
                    values.append(record)
        finally:
            self.lib.duckdb_destroy_result(c.byref(result))
        return time.perf_counter() - start, rows, values

    def close(self):
        self.lib.duckdb_disconnect(c.byref(self.connection))
        self.lib.duckdb_close(c.byref(self.db))
