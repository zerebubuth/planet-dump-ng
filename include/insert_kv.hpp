#ifndef INSERT_KV_HPP
#define INSERT_KV_HPP

#include <leveldb/slice.h>

template <typename T>
void insert_kv(T &t, const leveldb::Slice &key, const leveldb::Slice &val);

#endif /* INSERT_KV_HPP */
