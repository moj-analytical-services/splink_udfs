#include "trie/suffix_trie.hpp"
#include <cstring>
#include <utility>

namespace duckdb {

struct ParseCursor {
	const uint8_t *p;
	const uint8_t *end;
};

static inline bool ReadU32(ParseCursor &c, uint32_t &out) {
	if (DUCKDB_UNLIKELY(c.p + 4 > c.end)) {
		return false;
	}
	out = (uint32_t)c.p[0] | ((uint32_t)c.p[1] << 8) | ((uint32_t)c.p[2] << 16) | ((uint32_t)c.p[3] << 24);
	c.p += 4;
	return true;
}
static inline bool ReadBytes(ParseCursor &c, uint32_t n, const uint8_t *&ptr) {
	if (DUCKDB_UNLIKELY(c.p + n > c.end)) {
		return false;
	}
	ptr = c.p;
	c.p += n;
	return true;
}
static inline bool ReadString(ParseCursor &c, std::string &s) {
	uint32_t len = 0;
	if (!ReadU32(c, len)) {
		return false;
	}
	const uint8_t *ptr = nullptr;
	if (!ReadBytes(c, len, ptr)) {
		return false;
	}
	s.assign(reinterpret_cast<const char *>(ptr), reinterpret_cast<const char *>(ptr) + len);
	return true;
}

static bool ParseNodeQCK2(ParseCursor &c, ParsedTrie &out, PNode *&node_out) {
    auto node = make_uniq<PNode>();

    // cnt
    uint32_t cnt = 0;
    if (!ReadU32(c, cnt)) {
        return false;
    }
    node->cnt = cnt;

    // term
    uint32_t term = 0;
    if (!ReadU32(c, term)) {
        return false;
    }
    node->term = term;

    // uprn (64-bit LE encoded as two u32)
    uint32_t lo = 0;
    uint32_t hi = 0;
    if (!ReadU32(c, lo)) {
        return false;
    }
    if (!ReadU32(c, hi)) {
        return false;
    }
    uint64_t uprn = static_cast<uint64_t>(lo) | (static_cast<uint64_t>(hi) << 32);
    node->uprn = uprn;

    // nchildren
    uint32_t nchild = 0;
    if (!ReadU32(c, nchild)) {
        return false;
    }

    node->kids.reserve(nchild);
    for (uint32_t i = 0; i < nchild; ++i) {
        std::string tok;
        if (!ReadString(c, tok)) {
            return false;
        }
        PNode *child_ptr = nullptr;
        if (!ParseNodeQCK2(c, out, child_ptr)) {
            return false;
        }
        node->kids.emplace_back(std::move(tok), child_ptr);
    }

    out.arena.emplace_back(std::move(node));
    node_out = out.arena.back().get();
    return true;
}

std::unique_ptr<ParsedTrie> ParseQCK2(const string_t &blob) {
	auto data_ptr = reinterpret_cast<const uint8_t *>(blob.GetDataUnsafe());
	auto data_len = blob.GetSize();
	if (data_len < 5) {
		return nullptr;
	}

	ParseCursor cur {data_ptr, data_ptr + data_len};

    // header: magic
    uint32_t magic = 0;
    if (!ReadU32(cur, magic)) {
        return nullptr;
    }
    if (magic != QCK2_MAGIC) {
        return nullptr;
    }

	// header: flags
	if (DUCKDB_UNLIKELY(cur.p >= cur.end)) {
		return nullptr;
	}
    uint8_t flags = *cur.p++;
    if (flags != 0x00) {
        return nullptr;
    }

    auto parsed = make_uniq<ParsedTrie>();
    PNode *root_ptr = nullptr;
    if (!ParseNodeQCK2(cur, *parsed, root_ptr)) {
        return nullptr;
    }
    parsed->root = root_ptr;

	// Strict consumption (fail on trailing bytes)
	if (cur.p != cur.end) {
		return nullptr;
	}

	return std::move(parsed);
}

// CountTail removed with navigation helpers

} // namespace duckdb
