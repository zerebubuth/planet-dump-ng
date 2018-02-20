#ifndef HISTORY_FILTER_HPP
#define HISTORY_FILTER_HPP

#include <boost/scoped_ptr.hpp>
#include <boost/optional.hpp>
#include <boost/program_options.hpp>
#include <vector>
#include "output_writer.hpp"

/**
 * acts as an output_writer filter, removing all the deleted elements
 * and elements whose version number is not a maximum for their ID.
 */
template <typename T>
struct history_filter : public output_writer {
  history_filter(const std::string &, const boost::program_options::variables_map &, const user_map_t &, const boost::posix_time::ptime &, user_info_level, historical_versions, changeset_discussions);
  virtual ~history_filter();

  void changesets(const std::vector<changeset> &,
                  const std::vector<current_tag> &,
                  const std::vector<changeset_comment> &);
  void nodes(const std::vector<node> &, const std::vector<old_tag> &);
  void ways(const std::vector<way> &, const std::vector<way_node> &, const std::vector<old_tag> &);
  void relations(const std::vector<relation> &, const std::vector<relation_member> &, const std::vector<old_tag> &);
  void finish();

private:
  boost::scoped_ptr<T> m_writer;
  
  // when filtering the history and we reach the end of a block of nodes
  // ways or relations, we don't know whether the final element in the
  // block is a maximum-version element until we've seen the next block.
  // so we need to store the last-seen element in a block until we either
  // get a new block, or finish() is called.
  struct left_over_nodes {
    node n;
    std::vector<old_tag> tags;
  };
  struct left_over_ways {
    way w;
    std::vector<way_node> nodes;
    std::vector<old_tag> tags;
  };
  struct left_over_relations {
    relation r;
    std::vector<relation_member> members;
    std::vector<old_tag> tags;
  };

  boost::optional<left_over_nodes> m_left_over_nodes;
  boost::optional<left_over_ways> m_left_over_ways;
  boost::optional<left_over_relations> m_left_over_relations;
};

#endif /* HISTORY_FILTER_HPP */
