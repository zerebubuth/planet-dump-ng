#ifndef TABLE_EXTRACTOR_HPP
#define TABLE_EXTRACTOR_HPP

#include <string>
#include "dump_reader.hpp"
#include "extract_kv.hpp"
#include "unescape_copy_row.hpp"

template <typename R>
struct table_extractor {
  typedef R row_type;

  table_extractor(const std::string &table_name,
                  const std::string &dump_file)
    : m_reader(table_name, dump_file) {
  }

  void read() {
    size_t bytes = 0;
    row_type row;
    unescape_copy_row<dump_reader, row_type> filter(m_reader);
    extract_kv<row_type> extract;
    while ((bytes = filter.read(row)) > 0) {
      std::string key, val;
      extract(row, key, val);
      m_reader.put(key, val);
    }
  }

private:
  dump_reader m_reader;
};

#endif /* TABLE_EXTRACTOR_HPP */
