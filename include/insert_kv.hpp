#ifndef INSERT_KV_HPP
#define INSERT_KV_HPP

#include "config.h"

#ifdef HAVE_LEVELDB
#include <leveldb/slice.h>
typedef leveldb::Slice slice_t;
#else /* HAVE_LEVELDB */
#include <string>
typedef std::string slice_t;
#endif /* HAVE_LEVELDB */

template <typename T>
void insert_kv(T &t, const slice_t &key, const slice_t &val);

#endif /* INSERT_KV_HPP */
