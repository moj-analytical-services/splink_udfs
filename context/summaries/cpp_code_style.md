
# DuckDB Community Extension – C++ Style, Modernity & Layout Guide
*(updated July 2025)*

Below is consolidated guidance distilled from the most-used community extensions and DuckDB core review feedback. Everything here is **known to compile across DuckDB’s CI matrix** (Linux, macOS, Windows, WebAssembly) and to remain stable against future DuckDB releases.

---

## C++ Language Standard & Modernity

**Target C++11 for all code that touches DuckDB headers.**

| Do | Why |
|----|-----|
| `CMAKE_CXX_STANDARD 11` in `CMakeLists.txt`. | DuckDB core is built with 11; CI pipelines expect it. |
| **Optionally** raise the standard (e.g. 17) **in your own CMake** if you: <br>• test on every target DuckDB supports, and <br>• never depend on newer std-lib APIs inside files that include DuckDB headers. | Several official extensions ship with C++17 flags; just be sure they still build in CI. |

**Favour within C++11**

* Smart pointers (`std::unique_ptr`, rare `std::shared_ptr`) & RAII.
* Range-based `for` loops, `auto` for long iterator types, lambdas.
* `override`/`final` on virtual overrides.
* Explicit fixed-width integers (`int32_t`, `uint64_t`) and DuckDB’s `idx_t`.

**Avoid**

* Raw `new`/`delete`, C-style casts, global state, C++ RTTI hacks.

---

## Code Style & Formatting

**Use DuckDB’s `.clang-format`** (found in the extension template). It derives from the Google style **with a 120-column limit** and tab indentation (spaces for alignment). Run it in pre-commit or CI.

Naming rules:

| Element | Convention |
|---------|-------------|
| Files        | `snake_case.cpp` / `.hpp` |
| Classes      | `CamelCase` (`NanoarrowExtension`) |
| Functions    | `CamelCase` (`FinalizeCopyBind`) |
| Variables    | `snake_case` (`global_state`) |

Include order (top to bottom):

1. Corresponding header (`"my_extension.hpp"`)
2. C headers
3. C++ std headers
4. Other third-party headers (DuckDB, OpenSSL, …)
5. Project headers

Every header uses `#pragma once`. Always brace `if`, `else`, `for`, `while` blocks—even single-line.

---

## Directory & File Layout

```

your-extension/
├─ src/
│  ├─ include/                # public/internal headers
│  │   └─ your\_extension.hpp
│  └─ your\_extension.cpp      # DUCKDB\_EXTENSION\_MAIN, Load()
├─ test/sql/                  # SQLLogic tests
├─ CMakeLists.txt
├─ extension\_config.cmake     # DuckDB build hook
├─ vcpkg.json                 # optional deps
└─ README.md, LICENSE …

````

* **Namespaces:** put code in `namespace duckdb { … }`. For large projects nest once (`duckdb::ext_fuzzy`).
* `DUCKDB_EXTENSION_MAIN` must be defined **before** including DuckDB headers in your main `.cpp`.

---

## Best Practices & Patterns

### Favour

* **DuckDB utilities** – `duckdb::make_uniq`, `StringUtil`, exception classes (`InvalidInputException`, …).
* **Vectorised executors** – `UnaryExecutor`, `BinaryExecutor`, `TernaryExecutor` handle batching & NULLs.
* **State structs** – `FunctionData`, `GlobalTableFunctionState`, `LocalTableFunctionState` for thread safety.
* **Static linking via vcpkg** – declare deps in `vcpkg.json`; CI will fetch and link.

### Avoid

* Global mutable variables.
* Manual memory management – always allocate through DuckDB or smart pointers.
* Platform-specific APIs without `#ifdef` guards.
* Monolithic 2 000-line `.cpp` files—split by feature.

---

## Re-using DuckDB Functionality

There are **two safe categories** of reuse:

1. **Internal utility classes** (headers under `common/`, `execution/`, etc.) – call them directly.
   * e.g. `StringUtil::Lower`, `ListVector`.
2. **SQL-level built-ins** (`substr`, `date_trunc`, …) – **do not** call their private C++ impl.
   Use either:
   * a **SQL macro** that rewrites your function into the built-in expression, or
   * **`ExpressionExecutor` binding** (future-proof, shown below).

### Example – `first_two_characters(string)`

```cpp
static void FirstTwoCharacters(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &ctx = state.GetContext();
    vector<unique_ptr<Expression>> e;
    e.push_back(make_uniq<BoundReferenceExpression>(args.data[0].GetType(), 0)); // input
    e.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(1)));          // start
    e.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(2)));          // len

    auto expr = ScalarFunction::BindScalarFunction(ctx, "substr", std::move(e));
    ExpressionExecutor exec(ctx);
    exec.Execute(args, *expr, result);      // vectorised
}

auto fn = ScalarFunction("first_two_characters",
                         {LogicalType::VARCHAR},
                         LogicalType::VARCHAR,
                         FirstTwoCharacters);
ExtensionUtil::RegisterFunction(db.instance, fn);
````

---

## Memory, Errors & Thread Safety

* Allocate result strings with `StringVector::AddString` (DuckDB owns/free them).
* Throw DuckDB exceptions for user errors; use `D_ASSERT` for programmer mistakes.
* Never access shared mutable state without synchronisation; prefer per-thread `LocalTableFunctionState`.

---

## External Dependencies

Keep them lean; DuckDB’s DLL is often embedded. When needed:

1. Add to `vcpkg.json`.
2. Prefer small, portable, static-link-friendly libraries.
3. Verify builds on all CI targets.

For heavy compute you can implement the core in Rust/C/Go as a separate static lib and call it via C ABI; pass DuckDB’s `malloc`/`free` into that library to avoid allocator mismatches.

---

## Testing

Place SQLLogic tests under `test/sql/`. Cover:

* NULL behaviour, Unicode, long strings
* Performance edge cases (large lists for bigrams)
* Integration in larger queries

Run them in CI with DuckDB nightly builds.

---

## Quick Checklist

* [ ] C++11 default (higher std only with full CI).
* [ ] `clang-format` (120 cols) run.
* [ ] Namespaces under `duckdb`.
* [ ] Smart-pointer, RAII, no globals.
* [ ] Functions registered via `ExtensionUtil`.
* [ ] Built-ins reused via macro or `ExpressionExecutor`.
* [ ] Minimal, portable vcpkg deps.
* [ ] SQLLogic tests green.

Following these rules makes your extension look and feel like core DuckDB—robust today, stable tomorrow.

```
::contentReference[oaicite:0]{index=0}
```


### Concrete Example: Creating `first_two_characters(string)`

Let's implement a new scalar function, `first_two_characters(string)`, that reuses DuckDB's existing `substr` SQL function internally.

```cpp
// In your src/your_extension.cpp

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

// The implementation for our first_two_characters(string) function
static void FirstTwoCharactersFunc(DataChunk &args, ExpressionState &state, Vector &result) {
    // 1. Get the ClientContext from the ExpressionState. This is our gateway.
    auto &context = state.GetContext();

    // 2. Define the arguments for the `substr` function we want to call.
    // We want to call substr(input_string, 1, 2)
    vector<unique_ptr<Expression>> substr_args;

    // Argument 1: The input column from our own function's arguments.
    // We create an expression that refers to the first input column (at index 0).
    substr_args.push_back(make_uniq<BoundReferenceExpression>(args.data[0].GetType(), 0));

    // Argument 2: The starting position (a constant integer 1).
    substr_args.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(1)));

    // Argument 3: The length (a constant integer 2).
    substr_args.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(2)));

    // 3. Bind the `substr` function with our created arguments.
    // DuckDB will find the right `substr` overload based on the argument types.
    auto bound_substr_expr = ScalarFunction::BindScalarFunction(context, "substr", std::move(substr_args));

    // 4. Execute the bound expression.
    // The executor will run `substr` on our input DataChunk (`args`)
    // and write the output directly into our function's `result` Vector.
    ExpressionExecutor executor(context);
    executor.Execute(args, *bound_substr_expr, result);
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register our new scalar function
    auto my_scalar_function = ScalarFunction(
        "first_two_characters",         // Function name in SQL
        {LogicalType::VARCHAR},         // Input arguments: one string
        LogicalType::VARCHAR,           // Return type: a string
        FirstTwoCharactersFunc          // C++ function pointer
    );
    ExtensionUtil::RegisterFunction(instance, my_scalar_function);
}

// ... (Rest of your extension's Load, Name, and C-API entry points) ...

} // namespace duckdb
```

This example demonstrates the clean separation: you handle the logic of your new function while delegating the complex, optimized work of an existing SQL function back to the DuckDB engine itself, ensuring stability and performance.



The `query-farm-crypto` extension you provided is a fantastic case study. It contains both perfect examples of the best practices we've discussed and what could be considered a "counterexample" that is, in fact, an equally valid and powerful design pattern.

Here’s a breakdown of the examples within the `query-farm-crypto` extension:

### Example 1: The "Veneer" Pattern (Following the Guidance)

The C++ code in `src/crypto_extension.cpp` is a perfect illustration of how to correctly interface with DuckDB's vectorized execution engine. Instead of writing complex loops to iterate over `DataChunk`s, it uses DuckDB's built-in `BinaryExecutor` and `TernaryExecutor`.

The C++ code acts as a thin "veneer" or "shim" whose only job is to:
1.  Receive a `DataChunk` from the DuckDB engine.
2.  Use an executor to efficiently handle row-by-row iteration and nulls.
3.  For each row, call the core logic function (which happens to be in Rust).
4.  Handle the return value and place it correctly in the result vector.

**Source File:** `src/crypto_extension.cpp`

**Code Snippet:**

```cpp
inline void CryptoScalarHashFun(DataChunk &args, ExpressionState &state, Vector &result)
{
    auto &hash_name_vector = args.data[0];
    auto &value_vector = args.data[1];

    // PERFECT EXAMPLE: Using BinaryExecutor to handle iteration and nulls.
    BinaryExecutor::Execute<string_t, string_t, string_t>(
        hash_name_vector, value_vector, result, args.size(),
        [&](string_t hash_name, string_t value)
        {
            // The core logic is delegated to a clean C-style function call.
            auto hash_result = hashing_varchar(hash_name.GetData(), hash_name.GetSize(), value.GetData(), value.GetSize());

            if (hash_result.tag == ResultCString::Tag::Err)
            {
                // Correctly using DuckDB's exception system.
                throw InvalidInputException(hash_result.err._0);
            }

            // Correctly using DuckDB's string handling.
            auto output = StringVector::AddString(result, hash_result.ok._0);
            return output;
        });
}
```

This code expertly follows the guidance by reusing DuckDB's internal `BinaryExecutor`, `InvalidInputException`, and `StringVector` utilities.

### Counterexample (A Good One): Core Logic in Rust, Not C++

This extension does **not** reuse any of DuckDB's SQL-level functions (like `substr` or `date_trunc`). This is a "counterexample" to that specific piece of advice, but it's the correct design choice here for a very important reason: **the functionality it provides (cryptographic hashing) does not already exist in DuckDB.**

Instead of trying to implement complex cryptographic algorithms in C++, the developer chose to implement the core logic in Rust. This is an excellent pattern that leverages:
*   **Safety:** Rust's memory safety guarantees are a huge benefit for complex logic.
*   **Ecosystem:** The developer can instantly use mature, well-tested, and performant cryptographic libraries (crates) like `sha2`, `hmac`, and `digest`.
*   **Separation of Concerns:** The C++ code handles the DuckDB interface, and the Rust code handles the cryptographic computation.

**Source File:** `duckdb_crypto_rust/src/lib.rs`

**Code Snippet:**
```rust
// This Rust function contains the actual hashing logic.
// It is completely independent of DuckDB's internal C++ implementations.
fn select_hasher(s: &str) -> Option<Box<dyn DynDigest>> {
    match s {
        "blake2b-512" => Some(Box::<blake2::Blake2b512>::default()),
        "keccak256" => Some(Box::<sha3::Keccak256>::default()),
        // ... many other hash functions ...
        "sha2-256" => Some(Box::<sha2::Sha256>::default()),
        _ => None,
    }
}```

This demonstrates that when you are introducing *genuinely new, low-level functionality*, implementing it in a separate, compiled library (in this case, Rust) and calling it via a clean C interface is a superb and robust strategy.

### Example 2: Excellent Cross-Language Memory Management

This is a subtle but critical example of best practices. When the Rust code allocates memory for the result string that gets passed back to C++, how does it free that memory? If Rust frees it, DuckDB might still hold a pointer to it. If C++ tries to free it, it will cause a crash because the memory was allocated by a different allocator.

The `crypto` extension solves this beautifully by passing DuckDB's memory functions *to* the Rust library upon loading.

**Source File (C++ side):** `src/crypto_extension.cpp`
```cpp
static void LoadInternal(ExtensionLoader &loader)
{
    // HERE: Pass DuckDB's memory functions to the Rust library.
    init_memory_allocation(duckdb_malloc, duckdb_free);

    // ... register functions ...
}
```

**Source File (Rust side):** `duckdb_crypto_rust/src/lib.rs`
```rust
// A global static struct to hold the function pointers from DuckDB
static mut ALLOCATOR: Allocator = Allocator {
    malloc: None,
    free: None,
};

// The function exposed to C++ to receive the pointers
#[no_mangle]
pub extern "C" fn init_memory_allocation(
    malloc_fn: DuckDBMallocFunctionType,
    free_fn: DuckDBFreeFunctionType,
) {
    unsafe {
        INIT.call_once(|| {
            ALLOCATOR.malloc = Some(malloc_fn);
            ALLOCATOR.free = Some(free_fn);
        });
    }
}

// A helper function in Rust that uses DuckDB's allocator
fn create_cstring_with_custom_allocator(s: &str) -> CString {
    // ...
    unsafe {
        // HERE: Call the malloc function pointer we got from DuckDB
        let ptr = ALLOCATOR.malloc.unwrap()(layout.size()) as *mut c_char;
        // ...
        CString::from_raw(ptr)
    }
}
```

This ensures that all memory for results is allocated and freed by DuckDB's own memory manager, completely avoiding cross-language memory corruption, which is a common and difficult bug to solve. It's a premier example of deep, correct integration.



Yes, the `query-farm-lindel` extension is another outstanding case study that perfectly demonstrates these principles, including a more advanced and nuanced example of reusing a core engine component.

Here are the key examples and counterexamples from the `lindel` extension:

### Example 1: Core Logic in Rust (A Good "Counterexample")

Just like the `crypto` extension, `lindel` implements its core, novel functionality—the complex mathematics of Hilbert and Morton space-filling curves—in a separate, self-contained Rust library. This is a "counterexample" to reusing an existing SQL function, but it is the correct pattern because **DuckDB does not have a native `hilbert_encode` function to reuse.**

The developer correctly identifies that this specialized, mathematical logic is best handled by an external library (`lindel` crate) and uses C++ purely as the integration layer.

**Source File:** `src/lindel_extension.cpp`
**Code Snippet:**

```cpp
// Inside the C++ function `lindelEncodeArrayFunc`

// ... (code to get pointers to input and output data) ...

// This C++ code does not know or care how Hilbert encoding works.
// It simply calls the C-style function that is implemented in Rust.
auto encoder = bind_info.encoding_type == 0 ? hilbert_encode_u64_var : morton_encode_u64_var;

// ...
encoder((uint64_t *)(left_data_64 + left_offset), array_number_of_elements, result_data_u128 + i);
// ...
```

**What this shows:** The C++ code acts as a "veneer," preparing the data from DuckDB's `DataChunk` and passing raw pointers to the Rust library, which performs the actual computation. This is an excellent design that leverages the strengths of both languages.

### Example 2: Reusing the Expression Executor in a Bind Function (A Perfect Example)

This is a subtle but powerful example of correctly reusing an internal engine component. The `hilbert_decode` function takes constant arguments to specify its behavior (e.g., the number of elements to decode, whether to return floats). To get the value of these arguments during the binding phase, the code doesn't manually inspect the expression tree. Instead, it asks DuckDB's own **Expression Executor** to evaluate them.

**Source File:** `src/lindel_extension.cpp`
**Code Snippet:**

```cpp
// Inside the `lindelDecodeToArrayBind` function

// A helper lambda to get the value of a constant argument.
auto get_foldable_value = [&](size_t index, LogicalType expected_type, const string &error_msg) -> Value
{
    if (!arguments[index]->IsFoldable())
    {
        throw NotImplementedException(error_msg);
    }
    // HERE: The key part. It reuses the ExpressionExecutor to get the value.
    Value val = ExpressionExecutor::EvaluateScalar(context, *arguments[index]).CastAs(context, expected_type);
    if (val.IsNull())
    {
        throw NotImplementedException(error_msg + " expected a not-null value");
    }
    return val;
};

// Use the helper to get the values of the constant arguments.
auto return_number_of_parts = UTinyIntValue::Get(get_foldable_value(1, LogicalType::UTINYINT, ...));
auto return_float = BooleanValue::Get(get_foldable_value(2, LogicalType::BOOLEAN, ...));
```

**What this shows:** This is a perfect demonstration of the principle. Instead of trying to write custom logic to figure out what the literal value of `2` or `true` is in a SQL query, the extension delegates this task to the engine's own robust `ExpressionExecutor`. It reuses a core component for its intended purpose, leading to simpler and more correct code.

### Summary

The `lindel` extension is a model of good design for extensions that introduce novel, computationally intensive logic:

1.  **It correctly implements the "veneer" pattern,** keeping the C++ layer thin and focused on engine integration while delegating the core algorithm to a specialized Rust library.
2.  **It provides a sophisticated example of reusing an internal engine component,** the `ExpressionExecutor`, during the binding phase to handle function arguments correctly and robustly.


Yes, the `pdet-scrooge-mcduck` extension is packed with excellent examples and one very insightful "counterexample" that perfectly illustrate the principles of reusing DuckDB's internal components.

### Example 1: The `yahoo_finance` Scanner (Reusing the Entire Query Engine)

This is a masterclass in reusing DuckDB's capabilities. Instead of writing C++ code to make HTTP requests, parse JSON, and build `DataChunk`s manually, the `yahoo_finance` table function internally constructs and executes another SQL query that leverages the `json` and `httpfs` extensions.

It's essentially a pre-packaged, complex query.

**Source File:** `src/scanner/yahoo_finance.cpp`

**Code Snippet:**
```cpp
shared_ptr<Relation> GeneratePlan(YahooFunctionData &bind_data) {
    // ... logic to calculate date epochs ...

    // Constructs a URL to the Yahoo Finance API
    string url = "https://query2.finance.yahoo.com/v8/finance/chart/" +
               bind_data.symbol + "?period1=" + from + "&period2=" + to +
               "&interval=" + bind_data.interval + "&events=history";

    // HERE: Builds a complete SQL query string that uses the read_json table function
    string query =
        "SELECT '" + bind_data.symbol +
        "'as symbol, list_transform(chart.result[1].timestamp, x -> "
        "make_timestamp(x*1000000)::date) as date, "
        "chart.result[1].indicators.quote[1].open as open, "
        // ... more columns ...
        "FROM read_json('" + url + "');";

    // HERE: Executes the generated query using an internal connection to DuckDB's own planner
    return bind_data.conn->RelationFromQuery(query);
}

// ... in the Scan function ...
void YahooScanner::Scan(ClientContext &context, TableFunctionInput &data_p,
                        DataChunk &output) {
  auto &data = (YahooFunctionData &)*data_p.bind_data;
  if (!data.plan) {
    return;
  }
  // The "plan" is the result of the internal query. We just fetch from it.
  unique_ptr<QueryResult> res = data.plan->Execute();
  auto result_chunk = res->Fetch();
  if (!result_chunk) {
    return;
  }
  output.Move(*result_chunk);
  data.plan = GeneratePlan(data);
}
```

**What this shows:** This is the ultimate form of reuse. The developer didn't need to write a single line of HTTP client or JSON parsing code. They simply built a new SQL query as a string and delegated the entire execution—including network requests, JSON parsing, and data transformation (`list_transform`, `make_timestamp`)—to the DuckDB engine itself. This is incredibly robust, as it will automatically benefit from any future improvements to the `json` or `httpfs` extensions.

### Example 2: Function Aliasing with `volatility` (Reusing Function Implementations)

This example from the `scrooge` extension is a clever and highly idiomatic way to reuse an existing SQL function's implementation under a different name. Instead of re-implementing standard deviation, it simply finds the existing `stddev_pop` aggregate function in the catalog and registers a copy of it with the new name "volatility".

**Source File:** `src/functions/aliases.cpp`

**Code Snippet:**
```cpp
void Aliases::Register(Connection &conn, Catalog &catalog) {
  // Register Volatility

  // 1. Look up the existing aggregate function in the catalog.
  auto &stddev =
      catalog
          .GetEntry(*conn.context, CatalogType::AGGREGATE_FUNCTION_ENTRY,
                    DEFAULT_SCHEMA, "stddev_pop")
          .Cast<AggregateFunctionCatalogEntry>();

  // 2. Copy its function set.
  auto volatility = stddev.functions;

  // 3. Give it a new name.
  volatility.name = "volatility";

  // 4. Register the copy as a new function.
  CreateAggregateFunctionInfo volatility_info(volatility);
  catalog.CreateFunction(*conn.context, volatility_info);

  // (Repeats the same pattern for SMA/AVG)
  // ...
}
```

**What this shows:** This is a perfect example of reusing an existing implementation without touching any C++ code. The extension is simply manipulating the catalog, telling DuckDB "this new function name, `volatility`, should behave exactly like the existing function `stddev_pop`." It's efficient, clean, and guarantees consistency.

### Counterexample (A Justified One): `time_bucket`

The `scrooge` extension implements its own `time_bucket` function, which appears similar to DuckDB's built-in `date_trunc` function. On the surface, this looks like a counterexample to the guidance of reusing existing functions.

**Source File:** `src/functions/timebucket.cpp`
**Code Snippet:**
```cpp
void TimeBucketFunction(DataChunk &args, ExpressionState &state,
                        Vector &result) {
  // ...
  auto interval = Interval::GetMicro(interval_value.GetValue<interval_t>());
  // ...
  // Manual, low-level calculation of the time bucket
  result_ptr[i] =
      timestamp_ptr[i].value - (timestamp_ptr[i].value % interval);
  // ...
}
```
**Why this is a *good* counterexample:** The documentation for the function (found in the same source file) clarifies the reason for this:

> "The time_bucket function is similar to the standard PostgreSQL date_trunc function. **Unlike date_trunc, it allows for arbitrary time intervals** instead of second, minute, and hour intervals."

This is the perfect reason to write a new function instead of reusing an existing one. `date_trunc` could not fulfill the specific requirements of the extension (handling arbitrary intervals like `'5 minutes'` or `'36 hours'`). Therefore, the developer correctly implemented the necessary new logic. This highlights the full guidance: **Reuse where possible, but implement new logic when the existing components do not meet your needs.**


