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

struct changeset_comment {
  static const int num_keys = 2;
  static const std::vector<std::string> &column_names();

  int64_t changeset_id, author_id;
  std::string body;
  boost::posix_time::ptime created_at;
  bool visible;
};

BOOST_FUSION_ADAPT_STRUCT(
  changeset_comment,
  (int64_t, changeset_id)
  (boost::posix_time::ptime, created_at)
  (int64_t, author_id)
  (std::string, body)
  (bool, visible)
  )

struct changeset {
  static const int num_keys = 1;
  static const std::vector<std::string> &column_names();
  static const std::string table_name();
  static const std::string tag_table_name();
  static const std::string inner_table_name();

  typedef current_tag tag_type;
  typedef changeset_comment inner_type;

  int64_t id;
  int32_t uid;
  boost::posix_time::ptime created_at;
  boost::optional<int32_t> min_lat, max_lat, min_lon, max_lon;
  boost::posix_time::ptime closed_at;
  int32_t num_changes;
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
  (int32_t, num_changes)
  )

struct old_tag {
  static const int num_keys = 3;
  static const std::vector<std::string> &column_names();

  int64_t element_id, version;
  std::string key, value;
};

BOOST_FUSION_ADAPT_STRUCT(
  old_tag,
  (int64_t, element_id)
  (int64_t, version)
  (std::string, key)
  (std::string, value)
  )

struct node {
  static const int num_keys = 2;
  static const std::vector<std::string> &column_names();
  static const std::string table_name();
  static const std::string tag_table_name();
  static const std::string inner_table_name();

  typedef old_tag tag_type;
  typedef int inner_type;

  int64_t id, version, changeset_id;
  bool visible;
  boost::posix_time::ptime timestamp;
  boost::optional<int64_t> redaction_id;
  int32_t latitude, longitude;
};

BOOST_FUSION_ADAPT_STRUCT(
  node,
  (int64_t, id)
  (int64_t, version)
  (int64_t, changeset_id)
  (bool, visible)
  (boost::posix_time::ptime, timestamp)
  (boost::optional<int64_t>, redaction_id)
  (int32_t, latitude)
  (int32_t, longitude)
  )

struct way_node { 
  static const int num_keys = 3;
  static const std::vector<std::string> &column_names();

  int64_t way_id, version, sequence_id, node_id;
};

BOOST_FUSION_ADAPT_STRUCT(
  way_node,
  (int64_t, way_id)
  (int64_t, version)
  (int64_t, sequence_id)
  (int64_t, node_id)
  )

struct way {
  static const int num_keys = 2;
  static const std::vector<std::string> &column_names();
  static const std::string table_name();
  static const std::string tag_table_name();
  static const std::string inner_table_name();

  typedef old_tag tag_type;
  typedef way_node inner_type;

  int64_t id, version, changeset_id;
  bool visible;
  boost::posix_time::ptime timestamp;
  boost::optional<int64_t> redaction_id;
};

BOOST_FUSION_ADAPT_STRUCT(
  way,
  (int64_t, id)
  (int64_t, version)
  (int64_t, changeset_id)
  (bool, visible)
  (boost::posix_time::ptime, timestamp)
  (boost::optional<int64_t>, redaction_id)
  )

struct relation_member {
  static const int num_keys = 3;
  static const std::vector<std::string> &column_names();

  int64_t relation_id, version, sequence_id;
  nwr_enum member_type;
  int64_t member_id;
  std::string member_role;
};

BOOST_FUSION_ADAPT_STRUCT(
  relation_member,
  (int64_t, relation_id)
  (int64_t, version)
  (int64_t, sequence_id)
  (nwr_enum, member_type)
  (int64_t, member_id)
  (std::string, member_role)
  )

struct relation {
  static const int num_keys = 2;
  static const std::vector<std::string> &column_names();
  static const std::string table_name();
  static const std::string tag_table_name();
  static const std::string inner_table_name();

  typedef old_tag tag_type;
  typedef relation_member inner_type;

  int64_t id, version, changeset_id;
  bool visible;
  boost::posix_time::ptime timestamp;
  boost::optional<int64_t> redaction_id;
};

BOOST_FUSION_ADAPT_STRUCT(
  relation,
  (int64_t, id)
  (int64_t, version)
  (int64_t, changeset_id)
  (bool, visible)
  (boost::posix_time::ptime, timestamp)
  (boost::optional<int64_t>, redaction_id)
  )

#endif /* TYPES_HPP */
