#include <boost/tuple/tuple.hpp>
#include <boost/fusion/view/iterator_range.hpp>
#include <boost/fusion/include/iterator_range.hpp>
#include <boost/fusion/algorithm/iteration/fold.hpp>
#include <boost/fusion/include/fold.hpp>
#include <boost/fusion/adapted/struct/adapt_struct.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/fusion/algorithm/iteration/for_each.hpp>
#include <boost/fusion/include/for_each.hpp>
#include <boost/fusion/adapted/boost_tuple.hpp>

#include "extract_kv.hpp"
#include "types.hpp"
#include "time_epoch.hpp"

namespace bt = boost::posix_time;
namespace bf = boost::fusion;

namespace {

struct app_item {
  typedef int result_type;

  app_item(std::ostream &o) : out(o) {}

  int operator()(int, bool b) const {
    char c = b ? 1 : 0;
    out.write(&c, 1);
    return 0;
  }
 
  int operator()(int, int16_t i) const {
    uint16_t ii = htobe16(i);
    out.write((const char *)(&ii), sizeof(int16_t));
    return 0;
  }
  
  int operator()(int, int32_t i) const {
    uint32_t ii = htobe32(i);
    out.write((const char *)(&ii), sizeof(int32_t));
    return 0;
  }
  
  int operator()(int, int64_t i) const {
    uint64_t ii = htobe64(i);
    out.write((const char *)(&ii), sizeof(int64_t));
    return 0;
  }
  
  int operator()(int, uint16_t i) const {
    uint16_t ii = htobe16(i);
    out.write((const char *)(&ii), sizeof(uint16_t));
    return 0;
  }
  
  int operator()(int, uint32_t i) const {
    uint32_t ii = htobe32(i);
    out.write((const char *)(&ii), sizeof(uint32_t));
    return 0;
  }
  
  int operator()(int, uint64_t i) const {
    uint64_t ii = htobe64(i);
    out.write((const char *)(&ii), sizeof(uint64_t));
    return 0;
  }

  int operator()(int, double d) const {
    out.write((const char *)(&d), sizeof(double));
    return 0;
  }
  
  int operator()(int, const std::string &s) const {
    uint32_t size = s.size();
    if (size > size_t(std::numeric_limits<uint32_t>::max())) {
      throw std::runtime_error("String length too long.");
    }

    unsigned char c = 0;
    while (size > 0x7f) {
      c = 0x80 | (size & 0x7f);
      out.write((const char *)&c, 1);
      size >>= 7;
    }
    c = size & 0x7f;
    out.write((const char *)&c, 1);

    out.write(s.data(), s.size());
    return 0;
  }
  
  int operator()(int, const bt::ptime &t) const {
    if (t < time_epoch) {
      throw std::runtime_error("Time is before epoch.");
    }
    bt::time_duration dt = t - time_epoch;
    long seconds = dt.total_seconds();
    if (seconds > long(std::numeric_limits<uint32_t>::max())) {
      throw std::runtime_error("Time is too late after epoch.");
    }
    operator()(0, uint32_t(seconds));
    return 0;
  }
  
  template <typename T>
  int operator()(int, const boost::optional<T> &o) const {
    if (o) {
      out.put(0x01);
      operator()(0, o.get());
    } else {
      out.put(0x00);
    }
    return 0;
  }

  int operator()(int, user_status_enum e) const {
    char c = char(e);
    out.put(c);
    return 0;
  }

  int operator()(int, format_enum e) const {
    char c = char(e);
    out.put(c);
    return 0;
  }

  int operator()(int, nwr_enum e) const {
    char c = char(e);
    out.put(c);
    return 0;
  }

  std::ostream &out;
};

template <typename T>
std::string to_binary(std::ostringstream &out, const T &t) {
  out.clear();
  out.seekp(0);
  bf::fold(t, 0, app_item(out));
  return out.str();
}

} // anonymous namespace

template <typename T>
void extract_kv<T>::operator()(T &t, std::string &key, std::string &val) {
  static const int num_keys = T::num_keys;
  typedef typename bf::result_of::begin<T>::type it_begin;
  typedef typename bf::result_of::end<T>::type it_end;
  typedef typename bf::result_of::advance_c<it_begin, num_keys>::type it_key;
  
  it_begin v_begin(t, 0);
  it_key v_key(t, 0);
  it_end v_end(t, 0);
  
  key = to_binary(out, bf::iterator_range<it_begin, it_key>(v_begin, v_key));
  val = to_binary(out, bf::iterator_range<it_key, it_end>(v_key, v_end));
}

template <>
void extract_kv<way_node>::operator()(way_node &t, std::string &key, std::string &val) {
  boost::tuple<int64_t, int64_t> t_key(t.way_id, t.sequence_id);
  boost::tuple<int64_t> t_val(t.node_id);
  key = to_binary(out, t_key);
  val = to_binary(out, t_val);
}

template <>
void extract_kv<relation_member>::operator()(relation_member &t, std::string &key, std::string &val) {
  boost::tuple<int64_t, int32_t> t_key(t.relation_id, t.sequence_id);
  boost::tuple<nwr_enum, int64_t, std::string> t_val(t.member_type, t.member_id, t.member_role);
  key = to_binary(out, t_key);
  val = to_binary(out, t_val);
}

template struct extract_kv<user>;
template struct extract_kv<changeset>;
template struct extract_kv<current_tag>;
template struct extract_kv<old_tag>;
template struct extract_kv<node>;
template struct extract_kv<way>;
template struct extract_kv<relation>;
