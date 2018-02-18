#ifndef CHANGESET_FILTER_HPP
#define CHANGESET_FILTER_HPP

#include <boost/scoped_ptr.hpp>
#include <boost/optional.hpp>
#include <boost/program_options.hpp>
#include <vector>
#include "output_writer.hpp"

/**
 * acts as an output_writer filter, removing all elements except
 * changesets from the output. this allows us to easily re-use
 * the xml_writer to output changeset-only dumps.
 */
template <typename T>
struct changeset_filter : public output_writer {
  changeset_filter(const std::string &, const boost::program_options::variables_map &,
                   const user_map_t &, const boost::posix_time::ptime &, bool, bool = false);
  virtual ~changeset_filter();

  void changesets(const std::vector<changeset> &,
                  const std::vector<current_tag> &,
                  const std::vector<changeset_comment> &);
  void nodes(const std::vector<node> &, const std::vector<old_tag> &);
  void ways(const std::vector<way> &, const std::vector<way_node> &, const std::vector<old_tag> &);
  void relations(const std::vector<relation> &, const std::vector<relation_member> &, const std::vector<old_tag> &);
  void finish();

private:
  boost::scoped_ptr<T> m_writer;
};

#endif /* CHANGESET_FILTER_HPP */
