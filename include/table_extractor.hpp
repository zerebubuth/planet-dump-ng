#ifndef TABLE_EXTRACTOR_HPP
#define TABLE_EXTRACTOR_HPP

#include <string>
#include <boost/date_time/posix_time/ptime.hpp>
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

template <typename T>
boost::posix_time::ptime timestamp_of(const T &) {
  return boost::posix_time::ptime(boost::posix_time::neg_infin);
}

template <> boost::posix_time::ptime timestamp_of<changeset>(const changeset &cs)              { return cs.created_at; }
template <> boost::posix_time::ptime timestamp_of<current_node>(const current_node &n)         { return n.timestamp; }
template <> boost::posix_time::ptime timestamp_of<current_way>(const current_way &w)           { return w.timestamp; }
template <> boost::posix_time::ptime timestamp_of<current_relation>(const current_relation &r) { return r.timestamp; }

template <typename R>
struct table_extractor_with_timestamp {
  typedef R row_type;

  table_extractor_with_timestamp(const std::string &table_name,
                                 const std::string &dump_file)
    : m_reader(table_name, dump_file) {
  }

  boost::posix_time::ptime read() {
    boost::posix_time::ptime timestamp(boost::posix_time::neg_infin);
    size_t bytes = 0;
    row_type row;
    unescape_copy_row<dump_reader, row_type> filter(m_reader);
    extract_kv<row_type> extract;
    while ((bytes = filter.read(row)) > 0) {
      std::string key, val;
      extract(row, key, val);
      m_reader.put(key, val);
      if (timestamp_of<R>(row) > timestamp) {
        timestamp = timestamp_of<R>(row);
      }
    }
    return timestamp;
  }

private:
  dump_reader m_reader;
};

#endif /* TABLE_EXTRACTOR_HPP */
