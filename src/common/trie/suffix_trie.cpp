#include "trie/suffix_trie.hpp"
#include "duckdb/common/exception.hpp"
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

static bool ParseNode(ParseCursor &c, ParsedTrie &out, PNode *&node_out) {
	auto node = make_uniq<PNode>();

	// cnt
	uint32_t cnt = 0;
	if (!ReadU32(c, cnt)) {
		return false;
	}
	node->cnt = cnt;

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
		if (!ParseNode(c, out, child_ptr)) {
			return false;
		}
		node->kids.emplace_back(std::move(tok), child_ptr);
	}

	out.arena.emplace_back(std::move(node));
	node_out = out.arena.back().get();
	return true;
}

std::unique_ptr<ParsedTrie> ParseQCK1(const string_t &blob) {
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
	if (magic != QCK1_MAGIC) {
		return nullptr;
	}

	// header: flags
	if (DUCKDB_UNLIKELY(cur.p >= cur.end)) {
		return nullptr;
	}
	uint8_t flags = *cur.p++;
	if (flags != QCK1_FLAGS_EXPECTED) {
		return nullptr;
	}

	auto parsed = make_uniq<ParsedTrie>();
	PNode *root_ptr = nullptr;
	if (!ParseNode(cur, *parsed, root_ptr)) {
		return nullptr;
	}
	parsed->root = root_ptr;

	// Strict consumption (fail on trailing bytes)
	if (cur.p != cur.end) {
		return nullptr;
	}

	return std::move(parsed);
}

uint32_t CountTail(const ParsedTrie &pt, const std::vector<std::string> &tail_reversed) {
	const PNode *n = pt.root;
	if (!n) {
		return 0;
	}
	for (const auto &tok : tail_reversed) {
		const auto &kids = n->kids;
		size_t lo = 0, hi = kids.size();
		bool found = false;
		while (lo < hi) {
			size_t mid = (lo + hi) / 2;
			int cmp = tok.compare(kids[mid].first);
			if (cmp == 0) {
				n = kids[mid].second;
				found = true;
				break;
			}
			if (cmp < 0) {
				hi = mid;
			} else {
				lo = mid + 1;
			}
		}
		if (!found) {
			return 0;
		}
	}
	return n->cnt;
}

} // namespace duckdb
