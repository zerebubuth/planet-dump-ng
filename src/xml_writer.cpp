#include "xml_writer.hpp"
#include "config.h"
#include "writer_common.hpp"

#include <libxml/encoding.h>
#include <libxml/xmlwriter.h>

#include <stdexcept>
#include <boost/foreach.hpp>

#define SCALE (10000000)

namespace pt = boost::posix_time;

struct xml_writer::pimpl {
  pimpl(std::ostream &out, const pt::ptime &now);
  ~pimpl();

  void begin(const char *name);
  void attribute(const char *name, bool b);
  void attribute(const char *name, int32_t i);
  void attribute(const char *name, int64_t i);
  void attribute(const char *name, double d);
  void attribute(const char *name, const pt::ptime &t);
  void attribute(const char *name, const char *s);
  void attribute(const char *name, const std::string &s);
  void end();

  void add_tag(const current_tag &t);
  void add_tag(const old_tag &t);

  std::ostream &m_out;
  xmlTextWriterPtr m_writer;
  pt::ptime m_now;
};

static int wrap_write(void *context, const char *buffer, int len) {
  xml_writer::pimpl *impl = static_cast<xml_writer::pimpl *>(context);

  if (impl == NULL) {
    throw std::runtime_error("State object NULL in wrap_write.");
  }

  impl->m_out.write(buffer, len);
  if (impl->m_out.fail()) {
    throw std::runtime_error("Failed to write to output stream.");
  }
  return len;
}

static int wrap_close(void *context) {
  xml_writer::pimpl *impl = static_cast<xml_writer::pimpl *>(context);

  if (impl == NULL) {
    throw std::runtime_error("State object NULL in wrap_write.");
  }

  // NOTE: can't close std::ostream directly?
  return 0;
}

xml_writer::pimpl::pimpl(std::ostream &out, const pt::ptime &now) 
  : m_out(out), m_writer(NULL), m_now(now) {

  xmlOutputBufferPtr output_buffer =
    xmlOutputBufferCreateIO(wrap_write, wrap_close, this, NULL);

  m_writer = xmlNewTextWriter(output_buffer);
  if (m_writer == NULL) {
    free(output_buffer);

    throw std::runtime_error("Unable to create xmlTextWriter.");
  }

  xmlTextWriterSetIndent(m_writer, 1);
  if (xmlTextWriterStartDocument(m_writer, NULL, "UTF-8", NULL) < 0) {
    throw std::runtime_error("Unable to start document.");
  }
}

xml_writer::pimpl::~pimpl() {
  try {
    xmlTextWriterEndDocument(m_writer);
  } catch (...) {
  }
  xmlFreeTextWriter(m_writer);
}

void xml_writer::pimpl::begin(const char *name) {
  if (xmlTextWriterStartElement(m_writer, BAD_CAST name) < 0) {
    throw std::runtime_error("Unable to begin element XML.");
  }
}

void xml_writer::pimpl::attribute(const char *name, bool b) {
  const char *value = b ? "true" : "false";
  if (xmlTextWriterWriteAttribute(m_writer, 
                                  BAD_CAST name,
                                  BAD_CAST value) < 0) {
    throw std::runtime_error("Unable to write boolean attribute.");
  }
}

void xml_writer::pimpl::attribute(const char *name, int32_t i) {
  if (xmlTextWriterWriteFormatAttribute(m_writer, 
                                        BAD_CAST name,
                                        "%d", i) < 0) {
    throw std::runtime_error("Unable to write int32 attribute.");
  }
}

void xml_writer::pimpl::attribute(const char *name, int64_t i) {
  if (xmlTextWriterWriteFormatAttribute(m_writer, 
                                        BAD_CAST name,
                                        "%ld", i) < 0) {
    throw std::runtime_error("Unable to write int64 attribute.");
  }
}

void xml_writer::pimpl::attribute(const char *name, double d) {
  if (xmlTextWriterWriteFormatAttribute(m_writer, 
                                        BAD_CAST name,
                                        "%.7f", d) < 0) {
    throw std::runtime_error("Unable to write double attribute.");
  }
}

void xml_writer::pimpl::attribute(const char *name, const pt::ptime &t) {
  std::string ts = pt::to_iso_extended_string(t) + "Z";
  if (xmlTextWriterWriteAttribute(m_writer, 
                                  BAD_CAST name,
                                  BAD_CAST ts.c_str()) < 0) {
    throw std::runtime_error("Unable to write timestamp attribute.");
  }
}

void xml_writer::pimpl::attribute(const char *name, const char *s) {
  if (xmlTextWriterWriteAttribute(m_writer, 
                                  BAD_CAST name,
                                  BAD_CAST s) < 0) {
    throw std::runtime_error("Unable to write string attribute.");
  }
}

void xml_writer::pimpl::attribute(const char *name, const std::string &s) {
  if (xmlTextWriterWriteAttribute(m_writer, 
                                  BAD_CAST name,
                                  BAD_CAST s.c_str()) < 0) {
    throw std::runtime_error("Unable to write string attribute.");
  }
}

void xml_writer::pimpl::end() {
  if (xmlTextWriterEndElement(m_writer) < 0) {
    throw std::runtime_error("Unable to end element XML.");
  }
}

void xml_writer::pimpl::add_tag(const current_tag &t) {
  begin("tag");
  attribute("k", t.key);
  attribute("v", t.value);
  end();
}

void xml_writer::pimpl::add_tag(const old_tag &t) {
  begin("tag");
  attribute("k", t.key);
  attribute("v", t.value);
  end();
}

xml_writer::xml_writer(std::ostream &out, const user_map_t &users,
                       const pt::ptime &max_time)
  : m_impl(new pimpl(out, max_time)),
    m_users(users) {
  m_impl->begin("osm");
  m_impl->attribute("license",     OSM_LICENSE_TEXT);
  m_impl->attribute("copyright",   OSM_COPYRIGHT_TEXT);
  m_impl->attribute("version",     OSM_VERSION_TEXT);
  m_impl->attribute("generator",   PACKAGE_STRING);
  m_impl->attribute("attribution", OSM_ATTRIBUTION_TEXT);
  m_impl->attribute("timestamp",   m_impl->m_now);

  m_impl->begin("bound");
  m_impl->attribute("box", "-90,-180,90,180");
  m_impl->attribute("origin", OSM_API_ORIGIN);
  m_impl->end();
}

xml_writer::~xml_writer() {
  m_impl->end(); // </osm>
}

/*
void xml_writer::begin(const changeset &cs) {
  m_impl->begin("changeset");

  m_impl->attribute("id", cs.id);

  m_impl->attribute("created_at", cs.created_at);
  // yuck - nasty hack, but then we don't explicitly store closed time in
  // the database...
  const bool open = cs.closed_at > m_impl->m_now;
  if (!open) {
    m_impl->attribute("closed_at", cs.closed_at);
  }
  m_impl->attribute("open", open);

  user_map_t::const_iterator user_itr = m_users.find(cs.uid);
  if (user_itr != m_users.end()) {
    m_impl->attribute("user", user_itr->second);
    m_impl->attribute("uid", user_itr->first);
    m_changesets.insert(std::make_pair(cs.id, user_itr->first));
  }

  if (cs.min_lat && cs.max_lat && cs.min_lon && cs.max_lon) {
    m_impl->attribute("min_lat", double(cs.min_lat.get()) / SCALE);
    m_impl->attribute("min_lon", double(cs.min_lon.get()) / SCALE);
    m_impl->attribute("max_lat", double(cs.max_lat.get()) / SCALE);
    m_impl->attribute("max_lon", double(cs.max_lon.get()) / SCALE);
  }
}
*/

void xml_writer::nodes(const std::vector<node> &ns,
                       const std::vector<old_tag> &ts) {
  std::vector<old_tag>::const_iterator tag_itr = ts.begin();

  BOOST_FOREACH(const node &n, ns) {
    m_impl->begin("node");
    m_impl->attribute("id", n.id);
    m_impl->attribute("lat", double(n.latitude) / SCALE);
    m_impl->attribute("lon", double(n.longitude) / SCALE);
    m_impl->attribute("timestamp", n.timestamp);
    m_impl->attribute("version", n.version);
    m_impl->attribute("changeset", n.changeset_id);

    changeset_map_t::const_iterator cs_itr = m_changesets.find(n.changeset_id);
    if (cs_itr != m_changesets.end()) {
      user_map_t::const_iterator user_itr = m_users.find(cs_itr->second);
      if (user_itr != m_users.end()) {
        m_impl->attribute("user", user_itr->second);
        m_impl->attribute("uid", user_itr->first);
      }
    }

    while ((tag_itr != ts.end()) && (tag_itr->element_id <= n.id)) {
      if (tag_itr->element_id == n.id) {
        m_impl->add_tag(*tag_itr);
      }
      ++tag_itr;
    }

    m_impl->end();
  }
}

void xml_writer::ways(const std::vector<way> &ws,
                      const std::vector<way_node> &wns,
                      const std::vector<old_tag> &ts) {
  std::vector<old_tag>::const_iterator tag_itr = ts.begin();
  std::vector<way_node>::const_iterator nd_itr = wns.begin();

  BOOST_FOREACH(const way &w, ws) {
    m_impl->begin("way");
    m_impl->attribute("id", w.id);
    m_impl->attribute("timestamp", w.timestamp);
    m_impl->attribute("version", w.version);
    m_impl->attribute("changeset", w.changeset_id);
    
    changeset_map_t::const_iterator cs_itr = m_changesets.find(w.changeset_id);
    if (cs_itr != m_changesets.end()) {
      user_map_t::const_iterator user_itr = m_users.find(cs_itr->second);
      if (user_itr != m_users.end()) {
        m_impl->attribute("user", user_itr->second);
        m_impl->attribute("uid", user_itr->first);
      }
    }

    while ((nd_itr != wns.end()) && (nd_itr->way_id <= w.id)) {
      if (nd_itr->way_id == w.id) {
        m_impl->begin("nd");
        m_impl->attribute("ref", nd_itr->node_id);
        m_impl->end();
      }
      ++nd_itr;
    }

    while ((tag_itr != ts.end()) && (tag_itr->element_id <= w.id)) {
      if (tag_itr->element_id == w.id) {
        m_impl->add_tag(*tag_itr);
      }
      ++tag_itr;
    }

    m_impl->end();
  }
}

void xml_writer::relations(const std::vector<relation> &rs,
                           const std::vector<relation_member> &rms,
                           const std::vector<old_tag> &ts) {
  std::vector<old_tag>::const_iterator tag_itr = ts.begin();
  std::vector<relation_member>::const_iterator rm_itr = rms.begin();

  BOOST_FOREACH(const relation &r, rs) {
    m_impl->begin("relation");
    m_impl->attribute("id", r.id);
    m_impl->attribute("timestamp", r.timestamp);
    m_impl->attribute("version", r.version);
    m_impl->attribute("changeset", r.changeset_id);
    
    changeset_map_t::const_iterator cs_itr = m_changesets.find(r.changeset_id);
    if (cs_itr != m_changesets.end()) {
      user_map_t::const_iterator user_itr = m_users.find(cs_itr->second);
      if (user_itr != m_users.end()) {
        m_impl->attribute("user", user_itr->second);
        m_impl->attribute("uid", user_itr->first);
      }
    }

    while ((rm_itr != rms.end()) && (rm_itr->relation_id <= r.id)) {
      if (rm_itr->relation_id == r.id) {
        m_impl->begin("member");
        const char *type = 
          (rm_itr->member_id == nwr_node) ? "node" :
          (rm_itr->member_id == nwr_way) ? "way" :
          "relation";
        
        m_impl->attribute("type", type);
        m_impl->attribute("ref", rm_itr->member_id);
        m_impl->attribute("role", rm_itr->member_role);
        m_impl->end();
      }
      ++rm_itr;
    }

    while ((tag_itr != ts.end()) && (tag_itr->element_id <= r.id)) {
      if (tag_itr->element_id == r.id) {
        m_impl->add_tag(*tag_itr);
      }
      ++tag_itr;
    }

    m_impl->end();
  }
}

