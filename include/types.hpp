#ifndef TYPES_HPP
#define TYPES_HPP

#include <stdint.h>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/optional.hpp>
#include <boost/fusion/include/adapt_struct.hpp>

enum user_status_enum {
  user_status_pending,
  user_status_active,
  user_status_confirmed,
  user_status_suspended,
  user_status_deleted
};

enum format_enum {
  format_html,
  format_markdown,
  format_text
};

enum nwr_enum {
  nwr_node,
  nwr_way,
  nwr_relation
};

struct user {
  static const int num_keys = 1;
  static const std::vector<std::string> &column_names();

  int64_t id;
  std::string display_name;
  bool data_public;
};

BOOST_FUSION_ADAPT_STRUCT(
  user,
  (int64_t, id)
  (std::string, display_name)
  (bool, data_public)
  )
  
struct changeset {
  static const int num_keys = 1;
  static const std::vector<std::string> &column_names();

  int64_t id;
  int32_t uid;
  boost::posix_time::ptime created_at;
  boost::optional<int32_t> min_lat, max_lat, min_lon, max_lon;
  boost::posix_time::ptime closed_at;
};

BOOST_FUSION_ADAPT_STRUCT(
  changeset,
  (int64_t, id)
  (int32_t, uid)
  (boost::posix_time::ptime, created_at)
  (boost::optional<int32_t>, min_lat)
  (boost::optional<int32_t>, max_lat)
  (boost::optional<int32_t>, min_lon)
  (boost::optional<int32_t>, max_lon)
  (boost::posix_time::ptime, closed_at)
  )

struct current_tag {
  static const int num_keys = 2;
  static const std::vector<std::string> &column_names();

  int64_t element_id;
  std::string key, value;
};

BOOST_FUSION_ADAPT_STRUCT(
  current_tag,
  (int64_t, element_id)
  (std::string, key)
  (std::string, value)
  )

struct current_node {
  static const int num_keys = 1;
  static const std::vector<std::string> &column_names();

  int64_t id;
  int32_t latitude, longitude;
  int64_t changeset_id;
  bool visible;
  boost::posix_time::ptime timestamp;
  int64_t version;
};

BOOST_FUSION_ADAPT_STRUCT(
  current_node,
  (int64_t, id)
  (int32_t, latitude)
  (int32_t, longitude)
  (int64_t, changeset_id)
  (bool, visible)
  (boost::posix_time::ptime, timestamp)
  (int64_t, version)
  )

struct current_way {
  static const int num_keys = 1;
  static const std::vector<std::string> &column_names();

  int64_t id, changeset_id;
  boost::posix_time::ptime timestamp;
  bool visible;
  int64_t version;
};

BOOST_FUSION_ADAPT_STRUCT(
  current_way,
  (int64_t, id)
  (int64_t, changeset_id)
  (boost::posix_time::ptime, timestamp)
  (bool, visible)
  (int64_t, version)
  )

struct current_way_node { 
  static const std::vector<std::string> &column_names();

  int64_t way_id, node_id, sequence_id;
};

BOOST_FUSION_ADAPT_STRUCT(
  current_way_node,
  (int64_t, way_id)
  (int64_t, node_id)
  (int64_t, sequence_id)
  )

struct current_relation {
  static const int num_keys = 1;
  static const std::vector<std::string> &column_names();

  int64_t id, changeset_id;
  boost::posix_time::ptime timestamp;
  bool visible;
  int64_t version;
};

BOOST_FUSION_ADAPT_STRUCT(
  current_relation,
  (int64_t, id)
  (int64_t, changeset_id)
  (boost::posix_time::ptime, timestamp)
  (bool, visible)
  (int64_t, version)
  )

struct current_relation_member {
  static const std::vector<std::string> &column_names();

  int64_t relation_id;
  nwr_enum member_type;
  int64_t member_id;
  std::string member_role;
  int32_t sequence_id;
};

BOOST_FUSION_ADAPT_STRUCT(
  current_relation_member,
  (int64_t, relation_id)
  (nwr_enum, member_type)
  (int64_t, member_id)
  (std::string, member_role)
  (int32_t, sequence_id)
  )

#endif /* TYPES_HPP */
