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
    std::vector<std::string> columns;
    boost::split(columns, line, boost::is_any_of("\t"));
    if (columns.size() != s_num_columns) {
      size_t sz = s_num_columns;
      throw std::runtime_error((boost::format("Wrong number of columns: expecting %1%, got %2% in line `%3%'.") 
                                % sz % columns.size() % line).str());
    }
    try {
      set_values(row, columns);
    } catch (const std::exception &e) {
      throw std::runtime_error((boost::format("%1%: in line `%2%'.") % e.what() % line).str());
    }
  }

  inline void set_values(T &t, std::vector<std::string> &vs) {
    boost::fusion::for_each(t, set_value(vs.begin()));
  }

  struct set_value {
    explicit set_value(std::vector<std::string>::iterator i) : itr(i) {}

    void operator()(bool &b) const {
      std::string &str = *itr++;
      switch (str[0]) {
      case 't':
        b = true;
        break;
      case 'f':
        b = false;
        break;
      default:
        throw std::runtime_error((boost::format("Unrecognised value for bool: `%1%'") % str).str());
      }
    }

    void operator()(int16_t &i) const {
      std::string &str = *itr++;
      unescape(str);
      i = boost::lexical_cast<int16_t>(str);
    }
    
    void operator()(int32_t &i) const {
      std::string &str = *itr++;
      unescape(str);
      i = boost::lexical_cast<int32_t>(str);
    }
    
    void operator()(int64_t &i) const {
      std::string &str = *itr++;
      unescape(str);
      i = boost::lexical_cast<int64_t>(str);
    }

    void operator()(double &d) const {
      std::string &str = *itr++;
      unescape(str);
      d = boost::lexical_cast<double>(str);
    }

    void operator()(std::string &v) const {
      std::string &str = *itr++;
      std::swap(v, str);
    }

    void operator()(boost::posix_time::ptime &t) const {
      std::string &str = *itr++;
      unescape(str);
      t = boost::posix_time::time_from_string(str);
    }

    template <typename V>
    void operator()(boost::optional<V> &o) const {
      std::string &s = *itr;
      if (s == "\\N") {
        o = boost::none;
        ++itr;
      } else {
        V v;
        operator()(v);
        o = v;
      }
    }

    void operator()(user_status_enum &e) const {
      std::string &str = *itr++;
      unescape(str);
      if (str == "pending") {
        e = user_status_pending;
      } else if (str == "active") {
        e = user_status_active;
      } else if (str == "confirmed") {
        e = user_status_confirmed;
      } else if (str == "suspended") {
        e = user_status_suspended;
      } else if (str == "deleted") {
        e = user_status_deleted;
      } else {
        throw std::runtime_error((boost::format("Unrecognised value for user_status_enum: `%1%'.") % str).str());
      }
    }

    void operator()(format_enum &e) const {
      std::string &str = *itr++;
      unescape(str);
      if (str == "html") {
        e = format_html;
      } else if (str == "markdown") {
        e = format_markdown;
      } else if (str == "text") {
        e = format_text;
      } else {
        throw std::runtime_error((boost::format("Unrecognised value for format_enum: `%1%'.") % str).str());
      }
    }

    void operator()(nwr_enum &e) const {
      std::string &str = *itr++;
      unescape(str);
      if (str == "Node") {
        e = nwr_node;
      } else if (str == "Way") {
        e = nwr_way;
      } else if (str == "Relation") {
        e = nwr_relation;
      } else {
        throw std::runtime_error((boost::format("Unrecognised value for nwr_enum: `%1%'.") % str).str());
      }
    }

    void unescape(std::string &s) const {
    }

    mutable std::vector<std::string>::iterator itr;
  };

  S &m_source;
};

#endif /* UNESCAPE_COPY_ROW_HPP */
