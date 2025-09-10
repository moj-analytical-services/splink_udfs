#include "trie/trie_cache_utils.hpp"

namespace duckdb {

std::shared_ptr<const ParsedTrie> GetOrParseTrie(TrieCache &cache, const string_t &blob) {
    const auto data_ptr = reinterpret_cast<const uint8_t *>(blob.GetDataUnsafe());
    const size_t data_len = static_cast<size_t>(blob.GetSize());
    const uint64_t key = FNV1aHash64(data_ptr, data_len);

    auto got = cache.Get(key);
    if (got) {
        return got;
    }

    auto parsed = ParseQCK2(blob);
    if (!parsed) {
        return nullptr;
    }
    auto sp = std::shared_ptr<const ParsedTrie>(parsed.release());
    cache.Put(key, sp);
    return sp;
}

} // namespace duckdb
