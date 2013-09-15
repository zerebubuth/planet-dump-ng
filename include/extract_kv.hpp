#ifndef EXTRACT_KV_HPP
#define EXTRACT_KV_HPP

#include <string>
#include <sstream>

template <typename T>
struct extract_kv {
  void operator()(T &t, std::string &key, std::string &val);
private:
  std::ostringstream out;
};

#endif /* EXTRACT_KV_HPP */
