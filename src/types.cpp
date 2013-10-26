#include "types.hpp"

namespace {

const char *user_column_names_[] = { "id", "display_name", "data_public" };
const char *changeset_column_names_[] = { "id", "user_id", "created_at", "min_lat", "max_lat", "min_lon", "max_lon", "closed_at", "num_changes" };
const char *current_tag_column_names_[] = { "*", "k", "v" };
const char *old_tag_column_names_[] = { "*", "version", "k", "v" };
const char *node_column_names_[] = { "node_id", "version", "changeset_id", "visible", "timestamp", "redaction_id", "latitude", "longitude" };
const char *way_column_names_[] = { "way_id", "version", "changeset_id", "visible", "timestamp", "redaction_id" };
const char *way_node_column_names_[] = { "way_id", "version", "sequence_id", "node_id" };
const char *relation_column_names_[] = { "relation_id", "version", "changeset_id", "visible", "timestamp", "redaction_id" };
const char *relation_member_column_names_[] = { "relation_id", "version", "sequence_id", "member_type", "member_id", "member_role" };

const std::vector<std::string> user_column_names = std::vector<std::string>(user_column_names_, user_column_names_ + sizeof(user_column_names_) / sizeof(*user_column_names_));
const std::vector<std::string> changeset_column_names = std::vector<std::string>(changeset_column_names_, changeset_column_names_ + sizeof(changeset_column_names_) / sizeof(*changeset_column_names_));
const std::vector<std::string> current_tag_column_names = std::vector<std::string>(current_tag_column_names_, current_tag_column_names_ + sizeof(current_tag_column_names_) / sizeof(*current_tag_column_names_));
const std::vector<std::string> old_tag_column_names = std::vector<std::string>(old_tag_column_names_, old_tag_column_names_ + sizeof(old_tag_column_names_) / sizeof(*old_tag_column_names_));
const std::vector<std::string> node_column_names = std::vector<std::string>(node_column_names_, node_column_names_ + sizeof(node_column_names_) / sizeof(*node_column_names_));
const std::vector<std::string> way_column_names = std::vector<std::string>(way_column_names_, way_column_names_ + sizeof(way_column_names_) / sizeof(*way_column_names_));
const std::vector<std::string> way_node_column_names = std::vector<std::string>(way_node_column_names_, way_node_column_names_ + sizeof(way_node_column_names_) / sizeof(*way_node_column_names_));
const std::vector<std::string> relation_column_names = std::vector<std::string>(relation_column_names_, relation_column_names_ + sizeof(relation_column_names_) / sizeof(*relation_column_names_));
const std::vector<std::string> relation_member_column_names = std::vector<std::string>(relation_member_column_names_, relation_member_column_names_ + sizeof(relation_member_column_names_) / sizeof(*relation_member_column_names_));

} // anonymous namespace

const std::vector<std::string> &user::column_names()            { return user_column_names; }
const std::vector<std::string> &changeset::column_names()       { return changeset_column_names; }
const std::vector<std::string> &current_tag::column_names()     { return current_tag_column_names; }
const std::vector<std::string> &old_tag::column_names()         { return old_tag_column_names; }
const std::vector<std::string> &node::column_names()            { return node_column_names; }
const std::vector<std::string> &way::column_names()             { return way_column_names; }
const std::vector<std::string> &way_node::column_names()        { return way_node_column_names; }
const std::vector<std::string> &relation::column_names()        { return relation_column_names; }
const std::vector<std::string> &relation_member::column_names() { return relation_member_column_names; }

const std::string changeset::table_name() { return "changesets"; }
const std::string changeset::tag_table_name() { return "changeset_tags"; }
const std::string changeset::inner_table_name() { return ""; }

const std::string node::table_name() { return "nodes"; }
const std::string node::tag_table_name() { return "node_tags"; }
const std::string node::inner_table_name() { return ""; }

const std::string way::table_name() { return "ways"; }
const std::string way::tag_table_name() { return "way_tags"; }
const std::string way::inner_table_name() { return "way_nodes"; }

const std::string relation::table_name() { return "relations"; }
const std::string relation::tag_table_name() { return "relation_tags"; }
const std::string relation::inner_table_name() { return "relation_members"; }
