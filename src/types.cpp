#include "types.hpp"

namespace {

const char *user_column_names_[] = { "email", "id", "pass_crypt", "creation_time", "display_name", "data_public", "description",
                                     "home_lat", "home_lon", "home_zoom", "nearby", "pass_salt", "image_file_name", "email_valid",
                                     "new_email", "creation_ip", "languages", "status", "terms_agreed", "consider_pd", "openid_url",
                                     "preferred_editor", "terms_seen", "description_format", "image_fingerprint",
                                     "changesets_count", "traces_count", "diary_entries_count", "image_use_gravatar" };
const char *changeset_column_names_[] = { "id", "user_id", "created_at", "min_lat", "max_lat", "min_lon", "max_lon", "closed_at",
                                          "num_changes" };
const char *current_tag_column_names_[] = { "*", "k", "v" };
const char *current_node_column_names_[] = { "id", "latitude", "longitude", "changeset_id", "visible", "timestamp", "tile",
                                             "version" };
const char *current_way_column_names_[] = { "id", "changeset_id", "timestamp", "visible", "version" };
const char *current_way_node_column_names_[] = { "way_id", "node_id", "sequence_id" };
const char *current_relation_column_names_[] = { "id", "changeset_id", "timestamp", "visible", "version" };
const char *current_relation_member_column_names_[] = { "relation_id", "member_type", "member_id", "member_role", "sequence_id" };

const std::vector<std::string> user_column_names = std::vector<std::string>(user_column_names_, user_column_names_ + sizeof(user_column_names_) / sizeof(*user_column_names_));
const std::vector<std::string> changeset_column_names = std::vector<std::string>(changeset_column_names_, changeset_column_names_ + sizeof(changeset_column_names_) / sizeof(*changeset_column_names_));
const std::vector<std::string> current_tag_column_names = std::vector<std::string>(current_tag_column_names_, current_tag_column_names_ + sizeof(current_tag_column_names_) / sizeof(*current_tag_column_names_));
const std::vector<std::string> current_node_column_names = std::vector<std::string>(current_node_column_names_, current_node_column_names_ + sizeof(current_node_column_names_) / sizeof(*current_node_column_names_));
const std::vector<std::string> current_way_column_names = std::vector<std::string>(current_way_column_names_, current_way_column_names_ + sizeof(current_way_column_names_) / sizeof(*current_way_column_names_));
const std::vector<std::string> current_way_node_column_names = std::vector<std::string>(current_way_node_column_names_, current_way_node_column_names_ + sizeof(current_way_node_column_names_) / sizeof(*current_way_node_column_names_));
const std::vector<std::string> current_relation_column_names = std::vector<std::string>(current_relation_column_names_, current_relation_column_names_ + sizeof(current_relation_column_names_) / sizeof(*current_relation_column_names_));
const std::vector<std::string> current_relation_member_column_names = std::vector<std::string>(current_relation_member_column_names_, current_relation_member_column_names_ + sizeof(current_relation_member_column_names_) / sizeof(*current_relation_member_column_names_));

} // anonymous namespace

const std::vector<std::string> &user::column_names()                    { return user_column_names; }
const std::vector<std::string> &changeset::column_names()               { return changeset_column_names; }
const std::vector<std::string> &current_tag::column_names()             { return current_tag_column_names; }
const std::vector<std::string> &current_node::column_names()            { return current_node_column_names; }
const std::vector<std::string> &current_way::column_names()             { return current_way_column_names; }
const std::vector<std::string> &current_way_node::column_names()        { return current_way_node_column_names; }
const std::vector<std::string> &current_relation::column_names()        { return current_relation_column_names; }
const std::vector<std::string> &current_relation_member::column_names() { return current_relation_member_column_names; }

