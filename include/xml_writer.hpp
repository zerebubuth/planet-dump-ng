#ifndef XML_WRITER_HPP
#define XML_WRITER_HPP

#include "types.hpp"
#include <ostream>
#include <boost/scoped_ptr.hpp>
#include <map>
#include <string>

class xml_writer {
public:
  typedef std::map<int64_t, std::string> user_map_t;
  typedef std::map<int64_t, int64_t> changeset_map_t;

  xml_writer(std::ostream &, const user_map_t &);
  ~xml_writer();

  void begin(const changeset &);
  void begin(const current_node &);
  void begin(const current_way &);
  void begin(const current_relation &);

  void add(const current_tag &);
  void add(const current_way_node &);
  void add(const current_relation_member &);

  void end();

  struct pimpl;

private:
  boost::scoped_ptr<pimpl> m_impl;
  const user_map_t &m_users;
  changeset_map_t m_changesets;
};

#endif /* XML_WRITER_HPP */
