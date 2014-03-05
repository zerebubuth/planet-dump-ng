#include "xml_writer.hpp"
#include "config.h"
#include "writer_common.hpp"

#include <libxml/encoding.h>
#include <libxml/xmlwriter.h>

#include <stdexcept>
#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/exception/all.hpp>

#define SCALE (10000000)

namespace pt = boost::posix_time;

namespace {

struct shell_escape_char {
  template <typename ResultT>
  std::string operator()(const ResultT &result) const {
    std::string s;
    for (typename ResultT::const_iterator itr = result.begin();
         itr != result.end(); ++itr) {
      s += '\\';
      s += *itr;
    }
    return s;
  }
};

/**
 * according to http://www.w3.org/TR/xml/#charsets, there are a range of
 * characters which are valid UTF-8, but invalid XML. we remove some of
 * them here, mainly ASCII control characters, which otherwise choke
 * something which would read these files. note that libxml2 will
 * happily *output* them, which is also a problem for anyone reading
 * from the API :-(
 */
std::string kill_xml_bad_chars(const std::string &s) {
  const size_t size = s.size();
  std::string output(size, '\0');
  
  for (size_t i = 0; i < size; ++i) {
    char c = s[i];
    if ((c >= 0x00) && (c < 0x20) && (c != 0x09) && (c != 0x0a) && (c != 0x0d)) {
      // replace with question mark is same behaviour as existing planet
      // dump program.
      c = '?';
    }
    output[i] = c;
  }
  
  return output;
}

std::string popen_command(const std::string &file_name, const boost::program_options::variables_map &options) {
  std::string compress_command;
  try {
    compress_command = options["compress-command"].as<std::string>();
  } catch (...) {
    boost::throw_exception(
      boost::enable_error_info(
        std::runtime_error((boost::format("Unable to get options for \"%1%\".") % file_name).str()))
      << boost::errinfo_nested_exception(boost::current_exception()));
  }

  // need to shell escape the file name.
  // NOTE: this seems to be incredibly ill-defined, and varies depending on the
  // system shell. a better way would be to open the file directly and dup
  // the file descriptor, but that seems to be quite a pain in the arse.
  std::string escaped_file_name(file_name);
  boost::find_format_all(escaped_file_name, boost::token_finder(boost::is_any_of("\\\"")), shell_escape_char());

  std::ostringstream command;
  command << compress_command << " > \"" << escaped_file_name << "\"";
  return command.str();
}

} // anonymous namespace

struct xml_writer::pimpl {
  pimpl(const std::string &file_name, const boost::program_options::variables_map &options, const pt::ptime &now, bool has_history);
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

  // flush & close output stream
  void finish();

  std::string m_command;
  FILE *m_out;
  xmlTextWriterPtr m_writer;
  pt::ptime m_now;
  bool m_has_history;
};

static int wrap_write(void *context, const char *buffer, int len) {
  xml_writer::pimpl *impl = static_cast<xml_writer::pimpl *>(context);

  if (impl == NULL) {
    BOOST_THROW_EXCEPTION(std::runtime_error("State object NULL in wrap_write."));
  }
  if (impl->m_out == NULL) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Output pipe NULL in wrap_write."));
  }

  if (len < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Negative length in wrap_write."));
  }
  const size_t slen = len;

  const size_t status = fwrite(buffer, 1, slen, impl->m_out);
  if (status < slen) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Failed to write to output stream."));
  }
  return len;
}

static int wrap_close(void *context) {
  xml_writer::pimpl *impl = static_cast<xml_writer::pimpl *>(context);

  if (impl == NULL) {
    BOOST_THROW_EXCEPTION(std::runtime_error("State object NULL in wrap_close."));
  }
  if (impl->m_out == NULL) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Output pipe NULL in wrap_close."));
  }

  int status = pclose(impl->m_out);
  if (status == -1) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Output pipe could not be closed in wrap_close."));
  }
  impl->m_out = NULL;

  return 0;
}

xml_writer::pimpl::pimpl(const std::string &file_name, const boost::program_options::variables_map &options, const pt::ptime &now, bool has_history) 
  : m_command(popen_command(file_name, options)), m_out(popen(m_command.c_str(), "w")), 
    m_writer(NULL), m_now(now), m_has_history(has_history) {
  
  if (m_out == NULL) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to popen compression command for output."));
  }

  xmlOutputBufferPtr output_buffer =
    xmlOutputBufferCreateIO(wrap_write, wrap_close, this, NULL);

  m_writer = xmlNewTextWriter(output_buffer);
  if (m_writer == NULL) {
    free(output_buffer);

    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to create xmlTextWriter."));
  }

  xmlTextWriterSetIndent(m_writer, 1);
  if (xmlTextWriterStartDocument(m_writer, NULL, "UTF-8", NULL) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to start document."));
  }
}

xml_writer::pimpl::~pimpl() {
}

void xml_writer::pimpl::finish() {
  try {
    xmlTextWriterEndDocument(m_writer);
  } catch (...) {
  }
  xmlFreeTextWriter(m_writer);

  if (m_out != NULL) {
    // note that this *should* have already happened in xmlTextWriterEndDocument
    // but this is just to be on the safe side and not leave any processes
    // lying around.
    pclose(m_out);
  }
}

void xml_writer::pimpl::begin(const char *name) {
  if (xmlTextWriterStartElement(m_writer, BAD_CAST name) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to begin element XML."));
  }
}

void xml_writer::pimpl::attribute(const char *name, bool b) {
  const char *value = b ? "true" : "false";
  if (xmlTextWriterWriteAttribute(m_writer, 
                                  BAD_CAST name,
                                  BAD_CAST value) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to write boolean attribute."));
  }
}

void xml_writer::pimpl::attribute(const char *name, int32_t i) {
  if (xmlTextWriterWriteFormatAttribute(m_writer, 
                                        BAD_CAST name,
                                        "%d", i) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to write int32 attribute."));
  }
}

void xml_writer::pimpl::attribute(const char *name, int64_t i) {
  if (xmlTextWriterWriteFormatAttribute(m_writer, 
                                        BAD_CAST name,
                                        "%ld", i) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to write int64 attribute."));
  }
}

void xml_writer::pimpl::attribute(const char *name, double d) {
  if (xmlTextWriterWriteFormatAttribute(m_writer, 
                                        BAD_CAST name,
                                        "%.7f", d) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to write double attribute."));
  }
}

void xml_writer::pimpl::attribute(const char *name, const pt::ptime &t) {
  std::string ts = pt::to_iso_extended_string(t) + "Z";
  if (xmlTextWriterWriteAttribute(m_writer, 
                                  BAD_CAST name,
                                  BAD_CAST ts.c_str()) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to write timestamp attribute."));
  }
}

void xml_writer::pimpl::attribute(const char *name, const char *s) {
  std::string fixed = kill_xml_bad_chars(s);
  if (xmlTextWriterWriteAttribute(m_writer, 
                                  BAD_CAST name,
                                  BAD_CAST fixed.c_str()) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to write string attribute."));
  }
}

void xml_writer::pimpl::attribute(const char *name, const std::string &s) {
  std::string fixed = kill_xml_bad_chars(s);
  if (xmlTextWriterWriteAttribute(m_writer, 
                                  BAD_CAST name,
                                  BAD_CAST fixed.c_str()) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to write string attribute."));
  }
}

void xml_writer::pimpl::end() {
  if (xmlTextWriterEndElement(m_writer) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to end element XML."));
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

namespace {

/**
 * write attributes which are common to nodes, ways and relations.
 */
template <typename T>
void write_common_attributes(const T &t, xml_writer::pimpl &impl, 
                             const xml_writer::changeset_map_t &changesets,
                             const xml_writer::user_map_t &users) {
  impl.attribute("timestamp", t.timestamp);
  impl.attribute("version", t.version);
  impl.attribute("changeset", t.changeset_id);
  // it seems a "current" planet doesn't have visible attributes,
  // at least the current planetdump script doesn't add them.
  if (impl.m_has_history) { impl.attribute("visible", t.visible); }
  
  xml_writer::changeset_map_t::const_iterator cs_itr = changesets.find(t.changeset_id);
  if (cs_itr != changesets.end()) {
    xml_writer::user_map_t::const_iterator user_itr = users.find(cs_itr->second);
    if (user_itr != users.end()) {
      impl.attribute("user", user_itr->second);
      impl.attribute("uid", user_itr->first);
    }
  }
}

/**
 * write the tags which belong to a particular version of a node,
 * way or relation.
 */
void write_tags(int64_t id, int64_t version, 
                std::vector<old_tag>::const_iterator &tag_itr,
                const std::vector<old_tag>::const_iterator &end_itr,
                xml_writer::pimpl &impl) {
  while ((tag_itr != end_itr) && 
         ((tag_itr->element_id < id) ||
          ((tag_itr->element_id == id) &&
           (tag_itr->version <= version)))) {
    if ((tag_itr->element_id == id) && (tag_itr->version == version)) {
      impl.add_tag(*tag_itr);
    }
    ++tag_itr;
  }
}

} // anonymous namespace

xml_writer::xml_writer(const std::string &file_name, const boost::program_options::variables_map &options,
                       const user_map_t &users, const pt::ptime &max_time, bool has_history)
  : m_impl(new pimpl(file_name, options, max_time, has_history)), m_users(users) {
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
}

void xml_writer::changesets(const std::vector<changeset> &css,
                            const std::vector<current_tag> &ts) {
  std::vector<current_tag>::const_iterator tag_itr = ts.begin();

  BOOST_FOREACH(const changeset &cs, css) {
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

    m_impl->attribute("num_changes", cs.num_changes);

    while ((tag_itr != ts.end()) && (tag_itr->element_id <= cs.id)) {
      if (tag_itr->element_id == cs.id) {
        m_impl->add_tag(*tag_itr);
      }
      ++tag_itr;
    }

    m_impl->end();
  }
}

void xml_writer::nodes(const std::vector<node> &ns,
                       const std::vector<old_tag> &ts) {
  std::vector<old_tag>::const_iterator tag_itr = ts.begin();

  BOOST_FOREACH(const node &n, ns) {
    m_impl->begin("node");
    m_impl->attribute("id", n.id);
    // deleted nodes don't have lat/lon attributes
    if (n.visible) {
      m_impl->attribute("lat", double(n.latitude) / SCALE);
      m_impl->attribute("lon", double(n.longitude) / SCALE);
    }

    write_common_attributes<node>(n, *m_impl, m_changesets, m_users);

    // deleted nodes shouldn't have tags.
    if (n.visible) {
      write_tags(n.id, n.version, tag_itr, ts.end(), *m_impl);
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

    write_common_attributes<way>(w, *m_impl, m_changesets, m_users);

    // deleted ways shouldn't have nodes or tags, or at least we
    // shouldn't output them.
    if (w.visible) {
      while ((nd_itr != wns.end()) && 
             ((nd_itr->way_id < w.id) ||
              ((nd_itr->way_id == w.id) &&
               (nd_itr->version <= w.version)))) {
        if ((nd_itr->way_id == w.id) && (nd_itr->version == w.version)) {
          m_impl->begin("nd");
          m_impl->attribute("ref", nd_itr->node_id);
          m_impl->end();
        }
        ++nd_itr;
      }
      
      write_tags(w.id, w.version, tag_itr, ts.end(), *m_impl);
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
    write_common_attributes<relation>(r, *m_impl, m_changesets, m_users);

    // deleted relations don't have members or tags, or shouldn't have
    // them output anyway.
    if (r.visible) {
      while ((rm_itr != rms.end()) && 
             ((rm_itr->relation_id < r.id) ||
              ((rm_itr->relation_id == r.id) &&
               (rm_itr->version <= r.version)))) {
        if ((rm_itr->relation_id == r.id) && (rm_itr->version == r.version)) {
          m_impl->begin("member");
          const char *type = 
            (rm_itr->member_type == nwr_node) ? "node" :
            (rm_itr->member_type == nwr_way) ? "way" :
            "relation";
          
          m_impl->attribute("type", type);
          m_impl->attribute("ref", rm_itr->member_id);
          m_impl->attribute("role", rm_itr->member_role);
          m_impl->end();
        }
        ++rm_itr;
      }
      
      write_tags(r.id, r.version, tag_itr, ts.end(), *m_impl);
    }
    
    m_impl->end();
  }
}

void xml_writer::finish() {
  m_impl->end(); // </osm>
  m_impl->finish();
}
