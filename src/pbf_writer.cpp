#include "pbf_writer.hpp"
#include "config.h"
#include "writer_common.hpp"

#include <google/protobuf/io/gzip_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <osmpbf/osmpbf.h>

#include <boost/unordered_map.hpp>
#include <boost/foreach.hpp>

#include <zlib.h>
#include <arpa/inet.h>
#include <fstream>

namespace bt = boost::posix_time;

namespace {

struct string_table {
  typedef boost::unordered_map<std::string, int> string_map_t;

  string_table() : m_strings(), m_indexed_strings(), m_next_id(1) {}

  int operator()(const std::string &s) {
    string_map_t::iterator itr = m_strings.find(s);
    if (itr == m_strings.end()) {
      int key = m_next_id;
      ++m_next_id;
      m_strings.insert(std::make_pair(s, key));
      m_indexed_strings.push_back(s);
      return key;

    } else {
      return itr->second;
    }
  }

  void clear() {
    m_strings.clear();
    m_indexed_strings.clear();
    m_next_id = 1;
  }

  void write(OSMPBF::StringTable *st) const {
    // id 0 string is reserved for dense nodes, so just put an empty one in here.
    st->add_s("");
    for (std::vector<std::string>::const_iterator itr = m_indexed_strings.begin();
         itr != m_indexed_strings.end(); ++itr) {
      st->add_s(*itr);
    }
  }

  string_map_t m_strings;
  std::vector<std::string> m_indexed_strings;
  int m_next_id;
};

} // anonymous namespace

struct pbf_writer::pimpl {
  enum element_type {
    element_NULL,
    element_CHANGESET,
    element_NODE,
    element_WAY,
    element_RELATION
  };

  pimpl(const std::string &out_name, const bt::ptime &now, bool history_format,
        const user_map_t &user_map) 
    : num_elements(0), buffer(), out(out_name.c_str()), str_table(),
      pblock(), pgroup(pblock.add_primitivegroup()), 
      current_node(NULL), current_way(NULL), current_relation(NULL),
      m_byte_limit(int(0.25 * OSMPBF::max_uncompressed_blob_size)),
      m_current_element(element_NULL),
      m_last_way_node_ref(0),
      m_last_relation_member_ref(0),
      m_est_pblock_size(0),
      m_history_format(history_format),
      m_user_map(user_map),
      m_changeset_user_map() {
    write_header_block(now);
  }

  ~pimpl() {
  }

  void write_header_block(const bt::ptime &now) {
    using namespace OSMPBF;

    HeaderBlock header;
    HeaderBBox *bbox = header.mutable_bbox();
    bbox->set_left  (-180L * lonlat_resolution);
    bbox->set_right ( 180L * lonlat_resolution);
    bbox->set_top   (  90L * lonlat_resolution);
    bbox->set_bottom( -90L * lonlat_resolution);

    header.add_required_features("OsmSchema-V" OSM_VERSION_TEXT);
    if (m_history_format) { 
      header.add_required_features("HistoricalInformation");
    }
    header.add_optional_features("Has_Metadata");
    header.add_optional_features("Sort.Type_then_ID");
    header.set_writingprogram(PACKAGE_STRING);
    header.set_source(OSM_API_ORIGIN);
    header.set_osmosis_replication_timestamp((now - bt::from_time_t(time_t(0))).total_seconds());

    write_blob(header, "OSMHeader");
  }

  void write_blob(const google::protobuf::MessageLite &message, const std::string &type) {
    using namespace OSMPBF;
    using google::protobuf::io::StringOutputStream;
    using google::protobuf::io::GzipOutputStream;

    Blob blob;
    blob.set_raw_size(message.ByteSize());
    std::string str;
    StringOutputStream string_stream(&str);
    GzipOutputStream::Options options;
    options.format = GzipOutputStream::ZLIB;
    options.compression_level = 9;
    GzipOutputStream gzip_stream(&string_stream, options);
    message.SerializeToZeroCopyStream(&gzip_stream);
    gzip_stream.Close();
    blob.set_zlib_data(str);
    str.clear();

    BlobHeader blob_header;
    blob_header.set_type(type);
    blob_header.set_datasize(blob.ByteSize());
    uint32_t bh_size = htonl(blob_header.ByteSize());
    out.write((char *)&bh_size, sizeof bh_size);
    out << blob_header.SerializeAsString();
    out << blob.SerializeAsString();
    out.flush();
    std::cerr << "FLUSHED BLOCK" << std::endl;
  }

  void check_overflow(element_type type) {
    if ((m_current_element == element_NULL) ||
        (m_current_element == element_CHANGESET)) {  // <- to deal with OSMPBF brokenness...
      m_current_element = type;
    }

    if ((m_current_element != type) || (num_elements >= 16000)) {
      m_est_pblock_size += pgroup->ByteSize();
      bool new_block = (m_current_element != type) || (m_est_pblock_size >= m_byte_limit);
      if (new_block) {
        str_table.write(pblock.mutable_stringtable());
        write_blob(pblock, "OSMData");
        pblock.Clear();
        str_table.clear();
        
        m_current_element = type;
        m_est_pblock_size = 0;
      }

      pgroup = pblock.add_primitivegroup();
      num_elements = 0;
      current_node = NULL;
      current_way = NULL;
      current_relation = NULL;
    }
  }

  template <typename T>
  void set_info(const T &t, OSMPBF::Info *info) {
    static bt::ptime epoch = bt::from_time_t(time_t(0));
    
    info->set_version(t.version);
    info->set_timestamp((t.timestamp - epoch).total_seconds());
    info->set_changeset(t.changeset_id);
    // if we are doing a history file, and the default of visible=true
    // doesn't apply, then we need to explicitly set visible=false.
    if (m_history_format && !t.visible) {
      info->set_visible(t.visible);
    }
    // set the uid and user information, if the user is public
    std::map<int64_t, int64_t>::const_iterator itr = m_changeset_user_map.find(t.changeset_id);
    if (itr != m_changeset_user_map.end()) {
      user_map_t::const_iterator jtr = m_user_map.find(itr->second);
      if (jtr != m_user_map.end()) {
        info->set_uid(jtr->first);
        info->set_user_sid(str_table(jtr->second));
      }
    }
  }

  void add_changeset(const changeset &cs) {
    // looks like OSMPBF is broken and doesn't really support this.
    check_overflow(element_CHANGESET);
  }

  void add_node(const node &n) {
    check_overflow(element_NODE);

    current_node = pgroup->add_nodes();
    current_node->set_id(n.id);
    // deleted nodes don't have lat/lon attributes
    if (n.visible) {
      current_node->set_lat(n.latitude);
      current_node->set_lon(n.longitude);
    } else {
      // however, PBF doesn't allow you not to set these attributes,
      // so we have to set them to some null value. (0, 0) is, sadly,
      // the traditional value for these, even though it is valid.
      current_node->set_lat(0);
      current_node->set_lon(0);
    }
    set_info(n, current_node->mutable_info());

    ++num_elements;
  }

  void add_way(const way &w) {
    check_overflow(element_WAY);

    current_way = pgroup->add_ways();
    current_way->set_id(w.id);
    set_info(w, current_way->mutable_info());

    m_last_way_node_ref = 0;
    ++num_elements;
  }

  void add_relation(const relation &r) {
    check_overflow(element_RELATION);

    current_relation = pgroup->add_relations();
    current_relation->set_id(r.id);
    set_info(r, current_relation->mutable_info());

    m_last_relation_member_ref = 0;
    ++num_elements;
  }

  void add_tag(const old_tag &t) {
    if (m_current_element == element_NULL) {
      BOOST_THROW_EXCEPTION(std::runtime_error("Tag for NULL element type."));

    } else if (m_current_element == element_CHANGESET) {
      // OSMPBF brokenness - do nothing here.

    } else if (m_current_element == element_NODE) {
      if (current_node == NULL) { BOOST_THROW_EXCEPTION(std::runtime_error("Tag before node? oops.")); }
      current_node->add_keys(str_table(t.key));
      current_node->add_vals(str_table(t.value));

    } else if (m_current_element == element_WAY) {
      if (current_way == NULL) { BOOST_THROW_EXCEPTION(std::runtime_error("Tag before way? oops.")); }
      current_way->add_keys(str_table(t.key));
      current_way->add_vals(str_table(t.value));

    } else if (m_current_element == element_RELATION) {
      if (current_relation == NULL) { BOOST_THROW_EXCEPTION(std::runtime_error("Tag before relation? oops.")); }
      current_relation->add_keys(str_table(t.key));
      current_relation->add_vals(str_table(t.value));
    }
  }

  void add_way_node(const way_node &wn) {
    if (m_current_element != element_WAY) { BOOST_THROW_EXCEPTION(std::runtime_error("Unexpected way node.")); }
    current_way->add_refs(int64_t(wn.node_id) - m_last_way_node_ref);
    m_last_way_node_ref = wn.node_id;
  }

  OSMPBF::Relation::MemberType member_type(nwr_enum type) {
    switch (type) {
    case nwr_node:
      return OSMPBF::Relation::NODE;

    case nwr_way:
      return OSMPBF::Relation::WAY;
      
    case nwr_relation:
      return OSMPBF::Relation::RELATION;
    }

    throw std::runtime_error("Unknown nwr_enum value in member_type.");
  }

  void add_relation_member(const relation_member &rm) {
    if (m_current_element != element_RELATION) { BOOST_THROW_EXCEPTION(std::runtime_error("Unexpected relation member.")); }
    current_relation->add_roles_sid(str_table(rm.member_role));
    current_relation->add_memids(int64_t(rm.member_id) - m_last_relation_member_ref);
    current_relation->add_types(member_type(rm.member_type));
    m_last_relation_member_ref = rm.member_id;
  }
  
  void finish() {
    // flush out last remaining elements
    check_overflow(element_NULL);
    // and make sure it's all written out
    out.flush();
    // and finally close the file
    out.close();
  }

  size_t num_elements;
  std::ostringstream buffer;
  std::ofstream out;
  string_table str_table;
  OSMPBF::PrimitiveBlock pblock;
  OSMPBF::PrimitiveGroup *pgroup;
  OSMPBF::Node *current_node;
  OSMPBF::Way *current_way;
  OSMPBF::Relation *current_relation;
  const int m_byte_limit;
  element_type m_current_element;
  int64_t m_last_way_node_ref, m_last_relation_member_ref;
  int m_est_pblock_size;
  bool m_history_format;
  user_map_t m_user_map;
  std::map<int64_t, int64_t> m_changeset_user_map;

private:
  // noncopyable
  pimpl(const pimpl &);
  const pimpl &operator=(const pimpl &);
};

pbf_writer::pbf_writer(const std::string &file_name, const boost::program_options::variables_map &, 
                       const user_map_t &users, const boost::posix_time::ptime &now, bool history_format)
  : m_impl(new pimpl(file_name, now, history_format, users)) {
}

pbf_writer::~pbf_writer() {
}

void pbf_writer::changesets(const std::vector<changeset> &cs, const std::vector<current_tag> &) {
  std::map<int64_t, int64_t> &changeset_user_map = m_impl->m_changeset_user_map;
  BOOST_FOREACH(const changeset &c, cs) {
    changeset_user_map.insert(std::make_pair(c.id, c.uid));
  }
}

void pbf_writer::nodes(const std::vector<node> &ns,
                       const std::vector<old_tag> &ts) {
  std::vector<old_tag>::const_iterator tag_itr = ts.begin();

  BOOST_FOREACH(const node &n, ns) {
    m_impl->add_node(n);
    
    while ((tag_itr != ts.end()) && 
           ((tag_itr->element_id < n.id) ||
            ((tag_itr->element_id == n.id) &&
             (tag_itr->version <= n.version)))) {
      if ((tag_itr->element_id == n.id) && (tag_itr->version == n.version)) {
        m_impl->add_tag(*tag_itr);
      }
      ++tag_itr;
    }
  }    
}

void pbf_writer::ways(const std::vector<way> &ws,
                      const std::vector<way_node> &wns,
                      const std::vector<old_tag> &ts) {
  std::vector<old_tag>::const_iterator tag_itr = ts.begin();
  std::vector<way_node>::const_iterator nd_itr = wns.begin();

  BOOST_FOREACH(const way &w, ws) {
    m_impl->add_way(w);

    while ((nd_itr != wns.end()) && 
           ((nd_itr->way_id < w.id) ||
            ((nd_itr->way_id == w.id) &&
             (nd_itr->version <= w.version)))) {
      if ((nd_itr->way_id == w.id) && (nd_itr->version == w.version)) {
        m_impl->add_way_node(*nd_itr);
      }
      ++nd_itr;
    }

    while ((tag_itr != ts.end()) && 
           ((tag_itr->element_id < w.id) ||
            ((tag_itr->element_id == w.id) &&
             (tag_itr->version <= w.version)))) {
      if ((tag_itr->element_id == w.id) && (tag_itr->version == w.version)) {
        m_impl->add_tag(*tag_itr);
      }
      ++tag_itr;
    }
  }
}

void pbf_writer::relations(const std::vector<relation> &rs,
                           const std::vector<relation_member> &rms,
                           const std::vector<old_tag> &ts) {
  std::vector<old_tag>::const_iterator tag_itr = ts.begin();
  std::vector<relation_member>::const_iterator rm_itr = rms.begin();

  BOOST_FOREACH(const relation &r, rs) {
    m_impl->add_relation(r);

    while ((rm_itr != rms.end()) && 
           ((rm_itr->relation_id < r.id) ||
            ((rm_itr->relation_id == r.id) &&
             (rm_itr->version <= r.version)))) {
      if ((rm_itr->relation_id == r.id) && (rm_itr->version == r.version)) {
        m_impl->add_relation_member(*rm_itr);
      }
      ++rm_itr;
    }

    while ((tag_itr != ts.end()) && 
           ((tag_itr->element_id < r.id) ||
            ((tag_itr->element_id == r.id) &&
             (tag_itr->version <= r.version)))) {
      if ((tag_itr->element_id == r.id) && (tag_itr->version == r.version)) {
        m_impl->add_tag(*tag_itr);
      }
      ++tag_itr;
    }
  }
}

void pbf_writer::finish() {
  m_impl->finish();
}
