#pragma once
#include "duckdb.hpp"
#include <cstdint>
#include <list>
#include <memory>
#include <unordered_map>

#include "trie/suffix_trie.hpp"

namespace duckdb {

// FNV-1a 64-bit hash for raw bytes
inline uint64_t FNV1aHash64(const uint8_t *data, size_t len) {
	static constexpr uint64_t FNV_OFFSET_BASIS = 14695981039346656037ULL;
	static constexpr uint64_t FNV_PRIME = 1099511628211ULL;
	uint64_t hash = FNV_OFFSET_BASIS;
	for (size_t i = 0; i < len; ++i) {
		hash ^= data[i];
		hash *= FNV_PRIME;
	}
	return hash;
}

class TrieCache {
public:
	static constexpr size_t MAX_CACHE_SIZE = 64;
	using CacheKey = uint64_t;
	using CacheValue = std::shared_ptr<const ParsedTrie>;

private:
	struct CacheItem {
		CacheKey key;
		CacheValue value;
	};
	std::list<CacheItem> lru_list;
	std::unordered_map<CacheKey, typename std::list<CacheItem>::iterator> cache_map;

public:
	CacheValue Get(CacheKey key) {
		auto it = cache_map.find(key);
		if (it == cache_map.end()) {
			return nullptr;
		}
		lru_list.splice(lru_list.begin(), lru_list, it->second);
		return it->second->value;
	}
	void Put(CacheKey key, const CacheValue &value) {
		auto it = cache_map.find(key);
		if (it != cache_map.end()) {
			it->second->value = value;
			lru_list.splice(lru_list.begin(), lru_list, it->second);
			return;
		}
		lru_list.emplace_front(CacheItem {key, value});
		cache_map[key] = lru_list.begin();
		if (cache_map.size() > MAX_CACHE_SIZE) {
			auto &lru = lru_list.back();
			cache_map.erase(lru.key);
			lru_list.pop_back();
		}
	}
	size_t Size() const {
		return cache_map.size();
	}
};

} // namespace duckdb
