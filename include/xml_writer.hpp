#ifndef XML_WRITER_HPP
#define XML_WRITER_HPP

#include "output_writer.hpp"
#include <ostream>
#include <boost/scoped_ptr.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <map>
#include <string>

class xml_writer : public output_writer {
public:
  typedef std::map<int64_t, std::string> user_map_t;
  typedef std::map<int64_t, int64_t> changeset_map_t;

  xml_writer(std::ostream &, const user_map_t &, const boost::posix_time::ptime &);
  virtual ~xml_writer();

  void nodes(const std::vector<current_node> &, const std::vector<current_tag> &);
  void ways(const std::vector<current_way> &, const std::vector<current_way_node> &, const std::vector<current_tag> &);
  void relations(const std::vector<current_relation> &, const std::vector<current_relation_member> &, const std::vector<current_tag> &);

  struct pimpl;

private:
  boost::scoped_ptr<pimpl> m_impl;
  const user_map_t &m_users;
  changeset_map_t m_changesets;
};

#endif /* XML_WRITER_HPP */
