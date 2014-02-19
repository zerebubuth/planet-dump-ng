#include "changeset_map.hpp"
#include <boost/make_shared.hpp>

#define BLOCK_BITS 17
#define BLOCK_SIZE (1L << BLOCK_BITS)
#define BLOCK_MASK (BLOCK_SIZE - 1)

void changeset_map::insert(const changeset_map::value_type &kv) {
  assert(kv.first > 0);
  assert(kv.second >= 0);

  const size_t block_id = kv.first >> BLOCK_BITS;
  const size_t offset = kv.first & BLOCK_MASK;

  if (block_id >= m_data.size()) {
    m_data.resize(block_id + 1);
  }

  boost::shared_ptr<std::vector<int64_t> > ptr = m_data[block_id];
  if (!ptr) {
    ptr = boost::make_shared<std::vector<int64_t> >(BLOCK_SIZE, int64_t(-1));
    m_data[block_id] = ptr;
  }

  std::vector<int64_t> &vec = *ptr;
  vec[offset] = kv.second;
}

changeset_map::const_iterator changeset_map::find(int64_t k) const {
  if (k < 1) { return NULL; }

  const size_t block_id = k >> BLOCK_BITS;
  const size_t offset = k & BLOCK_MASK;

  if (block_id >= m_data.size()) {
    return NULL;
  }

  boost::shared_ptr<std::vector<int64_t> > ptr = m_data[block_id];
  if (!ptr) {
    return NULL;
  }

  std::vector<int64_t> &vec = *ptr;
  if (vec[offset] < 0) {
    return NULL;
  } else {
    return &vec[offset];
  }
}

changeset_map::const_iterator changeset_map::end() const {
  return NULL;
}
