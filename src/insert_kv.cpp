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

#include "insert_kv.hpp"
#include "types.hpp"
#include "time_epoch.hpp"

namespace bt = boost::posix_time;
namespace bf = boost::fusion;

namespace {

struct unapp_item {
  typedef int result_type;

  unapp_item(std::istream &i) : in(i) {}

  int operator()(int, bool &b) const {
    char c;
    in.read(&c, 1);
    b = c != 0;
    return 0;
  }

  int operator()(int, int16_t &i) const {
    uint16_t ii;
    in.read((char *)(&ii), sizeof(int16_t));
    i = be16toh(ii);
    return 0;
  }

  int operator()(int, int32_t &i) const {
    uint32_t ii;
    in.read((char *)(&ii), sizeof(int32_t));
    i = be32toh(ii);
    return 0;
  }

  int operator()(int, int64_t &i) const {
    uint64_t ii;
    in.read((char *)(&ii), sizeof(int64_t));
    i = be64toh(ii);
    return 0;
  }

  int operator()(int, uint16_t &i) const {
    uint16_t ii;
    in.read((char *)(&ii), sizeof(uint16_t));
    i = be16toh(ii);
    return 0;
  }

  int operator()(int, uint32_t &i) const {
    uint32_t ii;
    in.read((char *)(&ii), sizeof(uint32_t));
    i = be32toh(ii);
    return 0;
  }

  int operator()(int, uint64_t &i) const {
    uint64_t ii;
    in.read((char *)(&ii), sizeof(uint64_t));
    i = be64toh(ii);
    return 0;
  }

  int operator()(int, double &d) const {
    in.read((char *)(&d), sizeof(double));
    return 0;
  }

  int operator()(int, std::string &s) const {
    uint32_t size = 0;
    unsigned char c = 0;
    do {
      in.read((char *)&c, 1);
      size = (size << 7) | uint32_t(c & 0x7f);
    } while ((c & 0x80) == 0x80);

    s.resize(size);
    in.read(&s[0], size);
    return 0;
  }

  int operator()(int, bt::ptime &t) const {
    uint32_t dt;
    operator()(0, dt);
    t = time_epoch + bt::seconds(dt);
    return 0;
  }

  template <typename T>
  int operator()(int, boost::optional<T> &o) const {
    char c;
    in.get(c);
    if (c == 0) {
      o = boost::none;
    } else {
      T t;
      operator()(0, t);
      o = t;
    }
    return 0;
  }

  int operator()(int, user_status_enum &e) const {
    char c;
    in.read(&c, 1);
    e = user_status_enum(c);
    return 0;
  }

  int operator()(int, format_enum &e) const {
    char c;
    in.read(&c, 1);
    e = format_enum(c);
    return 0;
  }

  int operator()(int, nwr_enum &e) const {
    char c;
    in.read(&c, 1);
    e = nwr_enum(c);
    return 0;
  }    

  std::istream &in;
};

template <typename T>
void from_binary(const leveldb::Slice &s, T &t) {
  std::istringstream in(s.ToString());
  bf::fold(t, 0, unapp_item(in));
}

} // anonymous namespace

template <typename T>
void insert_kv(T &t, const leveldb::Slice &key, const leveldb::Slice &val) {
  static const int num_keys = T::num_keys;
  typedef typename bf::result_of::begin<T>::type it_begin;
  typedef typename bf::result_of::end<T>::type it_end;
  typedef typename bf::result_of::advance_c<it_begin, num_keys>::type it_key;

  it_begin v_begin(t, 0);
  it_key v_key(t, 0);
  it_end v_end(t, 0);

  bf::iterator_range<it_begin, it_key> key_range(v_begin, v_key);
  bf::iterator_range<it_key, it_end> val_range(v_key, v_end);

  from_binary(key, key_range);
  from_binary(val, val_range);
}

template <>
void insert_kv<current_way_node>(current_way_node &t, const leveldb::Slice &key, const leveldb::Slice &val) {
  boost::tuple<int64_t, int64_t> t_key;
  boost::tuple<int64_t> t_val;
  from_binary(key, t_key);
  from_binary(val, t_val);
  t.way_id = t_key.get<0>();
  t.node_id = t_val.get<0>();
  t.sequence_id = t_key.get<1>();
}

template <>
void insert_kv<current_relation_member>(current_relation_member &t, const leveldb::Slice &key, const leveldb::Slice &val) {
  boost::tuple<int64_t, int32_t> t_key;
  boost::tuple<nwr_enum, int64_t, std::string> t_val;
  from_binary(key, t_key);
  from_binary(val, t_val);
  t.relation_id = t_key.get<0>();
  t.member_type = t_val.get<0>();
  t.member_id = t_val.get<1>();
  t.member_role = t_val.get<2>();
  t.sequence_id = t_key.get<1>();
}

template void insert_kv<user>(user &, const leveldb::Slice &, const leveldb::Slice &);
template void insert_kv<changeset>(changeset &, const leveldb::Slice &, const leveldb::Slice &);
template void insert_kv<current_tag>(current_tag &, const leveldb::Slice &, const leveldb::Slice &);
template void insert_kv<current_node>(current_node &, const leveldb::Slice &, const leveldb::Slice &);
template void insert_kv<current_way>(current_way &, const leveldb::Slice &, const leveldb::Slice &);
template void insert_kv<current_relation>(current_relation &, const leveldb::Slice &, const leveldb::Slice &);
