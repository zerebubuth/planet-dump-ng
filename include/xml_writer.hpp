#ifndef XML_WRITER_HPP
#define XML_WRITER_HPP

#include "output_writer.hpp"
#include "changeset_map.hpp"
#include <ostream>
#include <boost/scoped_ptr.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/program_options.hpp>
#include <map>
#include <string>

class xml_writer : public output_writer {
public:
  typedef changeset_map changeset_map_t;

  xml_writer(const std::string &, const boost::program_options::variables_map &, const user_map_t &,
             const boost::posix_time::ptime &max_time,
             bool has_history = false, bool has_changeset_discussions = false);
  virtual ~xml_writer();

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
  const user_map_t &m_users;
  bool m_has_changeset_discussions;
  changeset_map_t m_changesets;
};

#endif /* XML_WRITER_HPP */
