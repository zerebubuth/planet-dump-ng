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

  std::string email;
  int64_t id;
  std::string pass_crypt;
  boost::posix_time::ptime creation_time;
  std::string display_name;
  bool data_public;
  std::string description;
  boost::optional<double> home_lat, home_lon;
  boost::optional<int16_t> home_zoom;
  boost::optional<int32_t> nearby;
  boost::optional<std::string> pass_salt, image_file_name;
  bool email_valid;
  boost::optional<std::string> new_email, creation_ip, languages;
  user_status_enum status;
  boost::optional<boost::posix_time::ptime> terms_agreed;
  bool consider_pd;
  boost::optional<std::string> preferred_editor;
  bool terms_seen;
  boost::optional<std::string> openid_url, image_fingerprint;
  format_enum description_format;
  int32_t changesets_count, traces_count, diary_entries_count;
  bool image_use_gravatar;
};

BOOST_FUSION_ADAPT_STRUCT(
  user,
  (std::string, email)
  (int64_t, id)
  (std::string, pass_crypt)
  (boost::posix_time::ptime, creation_time)
  (std::string, display_name)
  (bool, data_public)
  (std::string, description)
  (boost::optional<double>, home_lat)
  (boost::optional<double>, home_lon)
  (boost::optional<int16_t>, home_zoom)
  (boost::optional<int32_t>, nearby)
  (boost::optional<std::string>, pass_salt)
  (boost::optional<std::string>, image_file_name)
  (bool, email_valid)
  (boost::optional<std::string>, new_email)
  (boost::optional<std::string>, creation_ip)
  (boost::optional<std::string>, languages)
  (user_status_enum, status)
  (boost::optional<boost::posix_time::ptime>, terms_agreed)
  (bool, consider_pd)
  (boost::optional<std::string>, preferred_editor)
  (bool, terms_seen)
  (boost::optional<std::string>, openid_url)
  (boost::optional<std::string>, image_fingerprint)
  (format_enum, description_format)
  (int32_t, changesets_count)
  (int32_t, traces_count)
  (int32_t, diary_entries_count)
  (bool, image_use_gravatar)
  )
  
struct changeset {
  static const int num_keys = 1;

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

struct current_tag {
  static const int num_keys = 2;

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

  int64_t id;
  int32_t latitude, longitude;
  int64_t changeset_id;
  bool visible;
  boost::posix_time::ptime timestamp;
  int64_t tile, version;
};

BOOST_FUSION_ADAPT_STRUCT(
  current_node,
  (int64_t, id)
  (int32_t, latitude)
  (int32_t, longitude)
  (int64_t, changeset_id)
  (bool, visible)
  (boost::posix_time::ptime, timestamp)
  (int64_t, tile)
  (int64_t, version)
  )

struct current_way {
  static const int num_keys = 1;

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
