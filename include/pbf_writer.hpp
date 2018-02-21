#ifndef PBF_WRITER_HPP
#define PBF_WRITER_HPP

#include "output_writer.hpp"
#include <ostream>
#include <boost/scoped_ptr.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/program_options.hpp>
#include <map>
#include <string>

class pbf_writer : public output_writer {
public:
  pbf_writer(const std::string &, const boost::program_options::variables_map &, const user_map_t &, const boost::posix_time::ptime &, user_info_level, historical_versions, changeset_discussions);
  virtual ~pbf_writer();

  void changesets(const std::vector<changeset> &,
                  const std::vector<current_tag> &,
                  const std::vector<changeset_comment> &);
  void nodes(const std::vector<node> &, const std::vector<old_tag> &);
  void ways(const std::vector<way> &, const std::vector<way_node> &, const std::vector<old_tag> &);
  void relations(const std::vector<relation> &, const std::vector<relation_member> &, const std::vector<old_tag> &);
  void finish();

  struct pimpl;

private:
  boost::scoped_ptr<pimpl> m_impl;
};

#endif /* PBF_WRITER_HPP */
