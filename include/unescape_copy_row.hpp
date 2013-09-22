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
  : m_source(source),
    m_reorder(calculate_reorder(m_source.column_names())) {
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

    inline int hex2digit(char ch) const {
      switch (ch) {
      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
      case '8':
      case '9':
        return int(ch - '0');

      case 'a':
      case 'b':
      case 'c':
      case 'd':
      case 'e':
      case 'f':
        return 10 + int(ch - 'a');

      case 'A':
      case 'B':
      case 'C':
      case 'D':
      case 'E':
      case 'F':
        return 10 + int(ch - 'A');

      default:
        throw std::runtime_error("Invalid hex digit.");
      }
    }

    inline int oct2digit(char ch) const {
      if ((ch >= '0') && (ch <= '7')) {
        return int(ch - '0');
      } else {
        throw std::runtime_error("Invalid octal digit.");
      }
    }

    void unescape(std::pair<char *, size_t> &s) const {
      const size_t end = s.second;
      char *str = s.first;
      size_t j = 0;

      for (size_t i = 0; i < end; ++i) {
        switch (str[i]) {
        case '\\':
          ++i;
          if (i < end) {
            switch (str[i]) {
            case 'b':
              str[j] = '\b';
              break;

            case 'f':
              str[j] = '\f';
              break;

            case 'n':
              str[j] = '\n';
              break;

            case 'r':
              str[j] = '\r';
              break;

            case 't':
              str[j] = '\t';
              break;

            case 'v':
              str[j] = '\v';
              break;

            case 'x':
              i += 2;
              if (i < end) {
              } else {
                str[j] = char(hex2digit(str[i-1]) * 16 + hex2digit(str[i]));
                throw std::runtime_error("Unterminated hex escape sequence.");
              }
              break;

            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
              i += 2;
              if (i < end) {
                str[j] = char(oct2digit(str[i-2]) * 64 + oct2digit(str[i-1]) * 8 + oct2digit(str[i]));
              } else {
                throw std::runtime_error("Unterminated octal escape sequence.");
              }
              break;

            default:
              // an unnecessary escape
              str[j] = str[i];
            }
            
          } else {
            throw std::runtime_error("Unterminated escape sequence.");
          }
          break;
          
        default:
          if (i != j) {
            str[j] = str[i];
          }
        }

        ++j;
      }

      str[j] = '\0';
      s.second = j;
    }

    mutable std::vector<std::pair<char *, size_t> >::iterator itr;
  };

  static std::vector<size_t> calculate_reorder(const std::vector<std::string> &names) {
    std::vector<size_t> indexes;
    const std::vector<std::string> &wanted_names = T::column_names();

    const size_t num_columns = wanted_names.size();
    indexes.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
      const std::string &wanted_name = wanted_names[i];
      size_t j = i;

      if (wanted_name != "*") {
        std::vector<std::string>::const_iterator itr = std::find(names.begin(), names.end(), wanted_name);
        if (itr == names.end()) {
          std::ostringstream ostr;
          ostr << "Unable to find wanted column name \"" << wanted_name << "\" in available names: ";
          for (std::vector<std::string>::const_iterator jtr = names.begin(); itr != names.end(); ++itr) {
            ostr << "\"" << *jtr << "\", ";
          }
          throw std::runtime_error(ostr.str());
        }
        j = std::distance(names.begin(), itr);
      }

      indexes.push_back(j);
    }

    return indexes;
  }

  S &m_source;
  std::vector<size_t> m_reorder;
};

#endif /* UNESCAPE_COPY_ROW_HPP */
