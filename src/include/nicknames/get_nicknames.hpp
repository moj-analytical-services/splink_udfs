#pragma once

#include "duckdb.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#ifdef SPLINK_NICKNAME_TEST_HEADER
#include SPLINK_NICKNAME_TEST_HEADER
#else
#include "nicknames/nickname_data.hpp"
#endif

#include <algorithm>
#include <iterator>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>

namespace duckdb {
namespace nicknames {

// Unicode decoding and case/space tables are frozen independently of the host engine.
static uint32_t Decode(const char *data, idx_t size, idx_t &pos) {
	auto first = uint8_t(data[pos++]);
	if (first < 128) {
		return first;
	}
	idx_t count = first >= 0xc2 && first <= 0xdf   ? 1
	              : first <= 0xef && first >= 0xe0 ? 2
	              : first >= 0xf0 && first <= 0xf4 ? 3
	                                               : 0;
	if (!count || pos + count > size) {
		throw InvalidInputException("_get_nicknames: invalid UTF-8 input");
	}
	uint32_t cp = first & (0x7f >> (count + 1));
	for (idx_t i = 0; i < count; i++) {
		auto next = uint8_t(data[pos++]);
		if ((next & 0xc0) != 0x80) {
			throw InvalidInputException("_get_nicknames: invalid UTF-8 input");
		}
		cp = (cp << 6) | (next & 0x3f);
	}
	if (cp < (count == 1 ? 128U : count == 2 ? 2048U : 65536U) || cp > 0x10ffff || (cp >= 0xd800 && cp <= 0xdfff)) {
		throw InvalidInputException("_get_nicknames: invalid UTF-8 input");
	}
	return cp;
}

static void Encode(uint32_t cp, string &out) {
	if (cp < 128) {
		out.push_back(char(cp));
	} else if (cp < 2048) {
		out.push_back(char(0xc0 | (cp >> 6)));
		out.push_back(char(0x80 | (cp & 63)));
	} else if (cp < 65536) {
		out.push_back(char(0xe0 | (cp >> 12)));
		out.push_back(char(0x80 | ((cp >> 6) & 63)));
		out.push_back(char(0x80 | (cp & 63)));
	} else {
		out.push_back(char(0xf0 | (cp >> 18)));
		out.push_back(char(0x80 | ((cp >> 12) & 63)));
		out.push_back(char(0x80 | ((cp >> 6) & 63)));
		out.push_back(char(0x80 | (cp & 63)));
	}
}

static string NormalizeName(const string_t &input, const FrozenBehavior &behavior) {
	const auto data = input.GetData();
	const auto size = input.GetSize();
	idx_t begin = 0, end = size;
	while (begin < end && data[begin] == ' ') {
		begin++;
	}
	while (end > begin && data[end - 1] == ' ') {
		end--;
	}
	string normalized(data + begin, end - begin);
	bool ascii = true;
	for (auto &c : normalized) {
		if (uint8_t(c) >= 128) {
			ascii = false;
			break;
		}
		if (c >= 'A' && c <= 'Z') {
			c += 32;
		}
	}
	if (ascii) {
		return normalized;
	}
	normalized.clear();
	idx_t significant_end = 0;
	for (idx_t pos = 0; pos < size;) {
		auto cp = Decode(data, size, pos);
		if (std::binary_search(behavior.spaces, behavior.spaces + behavior.space_count, cp)) {
			if (!normalized.empty()) {
				Encode(cp, normalized);
			}
			continue;
		}
		if (cp >= 'A' && cp <= 'Z') {
			cp += 32;
		} else {
			auto mapping =
			    std::lower_bound(behavior.characters, behavior.characters + behavior.character_count, cp,
			                     [](const CharacterMapping &entry, uint32_t key) { return entry.source < key; });
			if (mapping != behavior.characters + behavior.character_count && mapping->source == cp) {
				cp = mapping->target;
			}
		}
		Encode(cp, normalized);
		significant_end = normalized.size();
	}
	normalized.resize(significant_end);
	return normalized;
}

using Pair = std::pair<uint32_t, uint32_t>;
static std::set<Pair> Reconstruct(idx_t version) {
	const auto &descriptor = VERSIONS[version];
	auto pairs = descriptor.parent < 0 ? std::set<Pair> {} : Reconstruct(idx_t(descriptor.parent));
	for (idx_t i = 0; i < descriptor.remove_count; i++) {
		const auto &entry = REMOVALS[descriptor.remove_offset + i];
		if (pairs.erase({entry.name, entry.nickname}) != 1) {
			throw InternalException("Invalid nickname removal");
		}
	}
	for (idx_t i = 0; i < descriptor.add_count; i++) {
		const auto &entry = ADDITIONS[descriptor.add_offset + i];
		if (!pairs.emplace(entry.name, entry.nickname).second) {
			throw InternalException("Duplicate nickname addition");
		}
	}
	if (pairs.size() != descriptor.mapping_count) {
		throw InternalException("Invalid nickname mapping count");
	}
	return pairs;
}

struct NicknameLookup {
	explicit NicknameLookup(idx_t version)
	    : behavior(BEHAVIORS[VERSIONS[version].behavior]),
	      values(LogicalType::LIST(LogicalType::VARCHAR), VERSIONS[version].mapping_count + 2) {
		auto pairs = Reconstruct(version);
		vector<Value> children;
		uint32_t previous = UINT32_MAX;
		for (const auto &pair : pairs) {
			if (pair.first != previous) {
				if (previous != UINT32_MAX) {
					values.SetValue(indices.size() - 1, Value::LIST(LogicalType::VARCHAR, std::move(children)));
					children = vector<Value> {};
				}
				indices.emplace(STRING_POOL + STRING_OFFSETS[pair.first], indices.size());
				previous = pair.first;
			}
			children.emplace_back(STRING_POOL + STRING_OFFSETS[pair.second]);
		}
		if (previous != UINT32_MAX) {
			values.SetValue(indices.size() - 1, Value::LIST(LogicalType::VARCHAR, std::move(children)));
		}
		empty_index = indices.size();
		null_index = empty_index + 1;
		FlatVector::SetSize(values, null_index + 1);
		values.SetValue(empty_index, Value::LIST(LogicalType::VARCHAR, vector<Value> {}));
		values.SetValue(null_index, Value(LogicalType::LIST(LogicalType::VARCHAR)));
	}
	const FrozenBehavior &behavior;
	std::unordered_map<string, idx_t> indices;
	Vector values;
	idx_t empty_index, null_index;
};

static std::shared_ptr<const NicknameLookup> GetLookup(idx_t version) {
	static std::mutex mutex;
	static vector<std::pair<idx_t, std::shared_ptr<const NicknameLookup>>> cache;
#ifdef SPLINK_NICKNAME_TEST_HEADER
	constexpr idx_t capacity = 1; // Exercise eviction while queries retain their bind data.
#else
	constexpr idx_t capacity = 4;
#endif
	std::lock_guard<std::mutex> guard(mutex);
	for (idx_t i = 0; i < cache.size(); i++) {
		if (cache[i].first == version) {
			auto entry = cache[i];
			cache.erase(cache.begin() + static_cast<ptrdiff_t>(i));
			cache.push_back(entry);
			return entry.second;
		}
	}
	auto lookup = std::make_shared<const NicknameLookup>(version);
	if (cache.size() == capacity) {
		cache.erase(cache.begin());
	}
	cache.emplace_back(version, lookup);
	return lookup;
}

struct NicknameBindData : FunctionData {
	NicknameBindData(idx_t version_p, std::shared_ptr<const NicknameLookup> lookup_p)
	    : version(version_p), lookup(std::move(lookup_p)) {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<NicknameBindData>(version, lookup);
	}
	bool Equals(const FunctionData &other) const override {
		return version == other.Cast<NicknameBindData>().version;
	}
	idx_t version;
	std::shared_ptr<const NicknameLookup> lookup;
};

static unique_ptr<FunctionData> Bind(BindScalarFunctionInput &input) {
	auto &argument = *input.GetArguments()[1];
	if (!argument.IsFoldable()) {
		throw BinderException("_get_nicknames: version must be a non-NULL constant resolved during binding");
	}
	auto value = ExpressionExecutor::EvaluateScalar(input.GetClientContext(), argument);
	if (value.IsNull()) {
		throw BinderException("_get_nicknames: version must be a non-NULL constant");
	}
	auto requested = value.GetValue<string>();
	string available;
	for (idx_t i = 0; i < std::size(VERSIONS); i++) {
		if (requested == VERSIONS[i].version) {
			return make_uniq<NicknameBindData>(i, GetLookup(i));
		}
		available += (i ? ", " : "") + string(VERSIONS[i].version);
	}
	throw BinderException("_get_nicknames: unknown version '%s'; available versions: %s", requested, available);
}

static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto &lookup = *state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<NicknameBindData>().lookup;
	UnifiedVectorFormat input;
	args.data[0].ToUnifiedFormat(input);
	const auto names = UnifiedVectorFormat::GetData<string_t>(input);
	SelectionVector selected(args.size());
	for (idx_t row = 0; row < args.size(); row++) {
		auto index = input.sel->get_index(row);
		idx_t match = lookup.null_index;
		if (input.validity.RowIsValid(index)) {
			auto found = lookup.indices.find(NormalizeName(names[index], lookup.behavior));
			match = found == lookup.indices.end() ? lookup.empty_index : found->second;
		}
		selected.set_index(row, match);
	}
	// Slice retains ownership of the immutable buffers even after cache eviction.
	result.Slice(lookup.values, selected, args.size());
}

struct VersionsState : GlobalTableFunctionState {
	idx_t position = 0;
};
static unique_ptr<FunctionData> VersionsBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types,
                                             vector<Identifier> &names) {
	for (const auto name : {"version", "upstream_repository", "upstream_commit", "source_file", "mapping_sha256",
	                        "behavior_id", "behavior_sha256"}) {
		names.emplace_back(name);
		types.push_back(LogicalType::VARCHAR);
	}
	names.emplace_back("mapping_count");
	types.push_back(LogicalType::BIGINT);
	return nullptr;
}
static unique_ptr<GlobalTableFunctionState> VersionsInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<VersionsState>();
}
static void VersionsExecute(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &position = input.global_state->Cast<VersionsState>().position;
	idx_t count = 0;
	while (position < std::size(VERSIONS) && count < STANDARD_VECTOR_SIZE) {
		const auto &v = VERSIONS[position++];
		const char *fields[] = {v.version,          v.repository, v.commit,
		                        v.source_file,      v.checksum,   BEHAVIORS[v.behavior].id,
		                        v.behavior_checksum};
		for (idx_t col = 0; col < std::size(fields); col++) {
			output.data[col].SetValue(count, Value(fields[col]));
		}
		output.data[7].SetValue(count++, Value::BIGINT(static_cast<int64_t>(v.mapping_count)));
	}
	for (auto &column : output.data) {
		FlatVector::SetSize(column, count);
	}
	output.CheckCardinality(count);
}

static void Register(ExtensionLoader &loader) {
	ScalarFunction function("_get_nicknames", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                        LogicalType::LIST(LogicalType::VARCHAR), Execute);
	function.SetBindCallback(Bind);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	function.SetFallible();
	loader.RegisterFunction(function);
	loader.RegisterFunction(TableFunction("nickname_lookup_versions", {}, VersionsExecute, VersionsBind, VersionsInit));
}

} // namespace nicknames
} // namespace duckdb
