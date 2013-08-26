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
    if (s.size() > std::numeric_limits<uint16_t>::max()) {
      throw std::runtime_error("String length too long.");
    }
    operator()(0, uint16_t(s.size()));
    out.write(s.data(), s.size());
    return 0;
  }
  
  int operator()(int, const bt::ptime &t) const {
    boost::gregorian::date::date_int_type days = t.date().day_number();
    bt::ptime::time_duration_type::tick_type ticks = t.time_of_day().ticks();
    operator()(0, days);
    operator()(0, ticks);
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
    return operator()(0, int(e));
  }

  int operator()(int, format_enum e) const {
    return operator()(0, int(e));
  }

  int operator()(int, nwr_enum e) const {
    return operator()(0, nwr_enum(e));
  }

  std::ostream &out;
};

template <typename T>
std::string to_binary(const T &t) {
  std::ostringstream out;
  bf::fold(t, 0, app_item(out));
  return out.str();
}

} // anonymous namespace

template <typename T>
void extract_kv(T &t, std::string &key, std::string &val) {
  static const int num_keys = T::num_keys;
  typedef typename bf::result_of::begin<T>::type it_begin;
  typedef typename bf::result_of::end<T>::type it_end;
  typedef typename bf::result_of::advance_c<it_begin, num_keys>::type it_key;

  it_begin v_begin(t, 0);
  it_key v_key(t, 0);
  it_end v_end(t, 0);

  key = to_binary(bf::iterator_range<it_begin, it_key>(v_begin, v_key));
  val = to_binary(bf::iterator_range<it_key, it_end>(v_key, v_end));
}

template <>
void extract_kv<current_way_node>(current_way_node &t, std::string &key, std::string &val) {
  boost::tuple<int64_t, int64_t> t_key(t.way_id, t.sequence_id);
  boost::tuple<int64_t> t_val(t.node_id);
  key = to_binary(t_key);
  val = to_binary(t_val);
}

template <>
void extract_kv<current_relation_member>(current_relation_member &t, std::string &key, std::string &val) {
  boost::tuple<int64_t, int32_t> t_key(t.relation_id, t.sequence_id);
  boost::tuple<nwr_enum, int64_t, std::string> t_val(t.member_type, t.member_id, t.member_role);
  key = to_binary(t_key);
  val = to_binary(t_val);
}

template void extract_kv<user>(user &, std::string &, std::string &);
template void extract_kv<changeset>(changeset &, std::string &, std::string &);
template void extract_kv<current_tag>(current_tag &, std::string &, std::string &);
template void extract_kv<current_node>(current_node &, std::string &, std::string &);
template void extract_kv<current_way>(current_way &, std::string &, std::string &);
template void extract_kv<current_relation>(current_relation &, std::string &, std::string &);
