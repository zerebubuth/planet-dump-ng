#ifndef INSERT_KV_HPP
#define INSERT_KV_HPP

#include "config.h"

#include <string>
typedef std::string slice_t;

template <typename T>
void insert_kv(T &t, const slice_t &key, const slice_t &val);

#endif /* INSERT_KV_HPP */
