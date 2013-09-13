#ifndef UNESCAPE_COPY_ROW_HPP
#define UNESCAPE_COPY_ROW_HPP

#include <boost/noncopyable.hpp>
#include <boost/fusion/include/size.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/optional.hpp>
#include <boost/fusion/include/for_each.hpp>

#include "types.hpp"

template <typename S, typename T>
struct unescape_copy_row 
  : public boost::noncopyable {
  static const size_t s_num_columns = boost::fusion::result_of::size<T>::value;

  explicit unescape_copy_row(S &source) 
  : m_source(source) {
  }

  ~unescape_copy_row() {
  }

  size_t read(T &row) {
    std::string line;
    size_t num = m_source.read(line);
    if (num > 0) {
      unpack(line, row);
    }
    return num;
  }

private:
  void unpack(std::string &line, T &row) {
    const size_t sz = s_num_columns;
    std::vector<std::pair<char *, size_t> > columns;
    columns.reserve(sz);
    {
      char *prev_ptr = &line[0];
      char * const end_ptr = &line[line.size()];
      char *ptr = &line[0];
      for (; ptr != end_ptr; ++ptr) {
        if (*ptr == '\t') {
          *ptr = '\0';
          columns.push_back(std::make_pair(prev_ptr, std::distance(prev_ptr, ptr)));
          prev_ptr = ptr + 1;
        }
      }
      columns.push_back(std::make_pair(prev_ptr, std::distance(prev_ptr, ptr)));
    }

    if (columns.size() != sz) {
      throw std::runtime_error((boost::format("Wrong number of columns: expecting %1%, got %2% in line `%3%'.") 
                                % sz % columns.size() % line).str());
    }
    try {
      set_values(row, columns);
    } catch (const std::exception &e) {
      throw std::runtime_error((boost::format("%1%: in line `%2%'.") % e.what() % line).str());
    }
  }

  inline void set_values(T &t, std::vector<std::pair<char *, size_t> > &vs) {
    boost::fusion::for_each(t, set_value(vs.begin()));
  }

  struct set_value {
    explicit set_value(std::vector<std::pair<char *, size_t> >::iterator i) : itr(i) {}

    void operator()(bool &b) const {
      std::pair<char *, size_t> str = *itr++;
      switch (str.first[0]) {
      case 't':
        b = true;
        break;
      case 'f':
        b = false;
        break;
      default:
        throw std::runtime_error((boost::format("Unrecognised value for bool: `%1%'") % str.first).str());
      }
    }

    void operator()(int16_t &i) const {
      std::pair<char *, size_t> str = *itr++;
      unescape(str);
      i = int16_t(strtol(str.first, NULL, 10));
    }
    
    void operator()(int32_t &i) const {
      std::pair<char *, size_t> str = *itr++;
      unescape(str);
      i = int32_t(strtol(str.first, NULL, 10));
    }
    
    void operator()(int64_t &i) const {
      std::pair<char *, size_t> str = *itr++;
      unescape(str);
      i = int64_t(strtoll(str.first, NULL, 10));
    }

    void operator()(double &d) const {
      std::pair<char *, size_t> str = *itr++;
      unescape(str);
      d = strtod(str.first, NULL);
    }

    void operator()(std::string &v) const {
      std::pair<char *, size_t> str = *itr++;
      v.assign(str.first, str.second);
    }

    void operator()(boost::posix_time::ptime &t) const {
      std::pair<char *, size_t> str = *itr++;
      unescape(str);
      //                    11111111112
      //           12345678901234567890
      // format is 2013-09-11 13:39:52.742365
      if (str.second < 19) { 
        throw std::runtime_error((boost::format("Unexpected format for timestamp: `%1%'.") 
                                  % str.first).str());
      }
      int year  = ((str.first[0] - '0') * 1000 +
                   (str.first[1] - '0') * 100 +
                   (str.first[2] - '0') * 10 +
                   (str.first[3] - '0'));
      int month = ((str.first[5] - '0') * 10 + (str.first[6] - '0'));
      int day   = ((str.first[8] - '0') * 10 + (str.first[9] - '0'));
      int hour  = ((str.first[11] - '0') * 10 + (str.first[12] - '0'));
      int min   = ((str.first[14] - '0') * 10 + (str.first[15] - '0'));
      int sec   = ((str.first[17] - '0') * 10 + (str.first[19] - '0'));
      t = boost::posix_time::ptime(boost::gregorian::date(year, month, day),
                                   boost::posix_time::time_duration(hour, min, sec));
    }

    template <typename V>
    void operator()(boost::optional<V> &o) const {
      std::pair<char *, size_t> s = *itr;
      if (strncmp(s.first, "\\N", s.second) == 0) {
        o = boost::none;
        ++itr;
      } else {
        V v;
        operator()(v);
        o = v;
      }
    }

    void operator()(user_status_enum &e) const {
      std::pair<char *, size_t> str = *itr++;
      unescape(str);
      if (strncmp(str.first, "pending", str.second) == 0) {
        e = user_status_pending;
      } else if (strncmp(str.first, "active", str.second) == 0) {
        e = user_status_active;
      } else if (strncmp(str.first, "confirmed", str.second) == 0) {
        e = user_status_confirmed;
      } else if (strncmp(str.first, "suspended", str.second) == 0) {
        e = user_status_suspended;
      } else if (strncmp(str.first, "deleted", str.second) == 0) {
        e = user_status_deleted;
      } else {
        throw std::runtime_error((boost::format("Unrecognised value for user_status_enum: `%1%'.") % str.first).str());
      }
    }

    void operator()(format_enum &e) const {
      std::pair<char *, size_t> str = *itr++;
      unescape(str);
      if (strncmp(str.first, "html", str.second) == 0) {
        e = format_html;
      } else if (strncmp(str.first, "markdown", str.second) == 0) {
        e = format_markdown;
      } else if (strncmp(str.first, "text", str.second) == 0) {
        e = format_text;
      } else {
        throw std::runtime_error((boost::format("Unrecognised value for format_enum: `%1%'.") % str.first).str());
      }
    }

    void operator()(nwr_enum &e) const {
      std::pair<char *, size_t> str = *itr++;
      unescape(str);
      if (strncmp(str.first, "Node", str.second) == 0) {
        e = nwr_node;
      } else if (strncmp(str.first, "Way", str.second) == 0) {
        e = nwr_way;
      } else if (strncmp(str.first, "Relation", str.second) == 0) {
        e = nwr_relation;
      } else {
        throw std::runtime_error((boost::format("Unrecognised value for nwr_enum: `%1%'.") % str.first).str());
      }
    }

    void unescape(std::pair<char *, size_t> &s) const {
    }

    mutable std::vector<std::pair<char *, size_t> >::iterator itr;
  };

  S &m_source;
};

#endif /* UNESCAPE_COPY_ROW_HPP */
