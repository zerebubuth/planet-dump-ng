#include "pbf_writer.hpp"
#include "config.h"
#include "writer_common.hpp"

#include <google/protobuf/io/gzip_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <osmpbf/osmpbf.h>

#include <boost/unordered_map.hpp>

#include <zlib.h>
#include <arpa/inet.h>

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

template <typename T>
void set_info(const T &t, OSMPBF::Info *info) {
  static bt::ptime epoch = bt::from_time_t(time_t(0));

  info->set_version(t.version);
  info->set_timestamp((t.timestamp - epoch).total_seconds());
  info->set_changeset(t.changeset_id);
}

} // anonymous namespace

struct pbf_writer::pimpl {
  enum element_type {
    element_NULL,
    element_CHANGESET,
    element_NODE,
    element_WAY,
    element_RELATION
  };

  pimpl(std::ostream &out_, const bt::ptime &now) 
    : num_elements(0), buffer(), out(out_), str_table(),
      pblock(), pgroup(pblock.add_primitivegroup()), 
      current_node(NULL), current_way(NULL), current_relation(NULL),
      m_byte_limit(size_t(0.25 * OSMPBF::max_uncompressed_blob_size)),
      m_current_element(element_NULL),
      m_last_way_node_ref(0),
      m_last_relation_member_ref(0) {
    write_header_block(now);
  }

  ~pimpl() {
    // flush out last remaining elements
    check_overflow(element_NULL);
    // and make sure it's all written out
    out.flush();
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

    // std::cerr << "current_element: " << m_current_element
    //           << ", type: " << type
    //           << ", num_elements: " << num_elements
    //           << ", bytesize(): " << pblock.ByteSize()
    //           << ", limit: " << m_byte_limit
    //           << std::endl;
    if ((m_current_element != type) || (num_elements >= 16000)) {
      bool new_block = (m_current_element != type) || (pblock.ByteSize() >= m_byte_limit);
      if (new_block) {
        str_table.write(pblock.mutable_stringtable());
        write_blob(pblock, "OSMData");
        pblock.Clear();
        str_table.clear();
        
        m_current_element = type;
      }

      pgroup = pblock.add_primitivegroup();
      num_elements = 0;
      current_node = NULL;
      current_way = NULL;
      current_relation = NULL;
    }
  }

  void add_changeset(const changeset &cs) {
    // looks like OSMPBF is broken and doesn't really support this.
    check_overflow(element_CHANGESET);
  }

  void add_node(const current_node &n) {
    check_overflow(element_NODE);

    current_node = pgroup->add_nodes();
    current_node->set_id(n.id);
    current_node->set_lat(n.latitude);
    current_node->set_lon(n.longitude);
    set_info(n, current_node->mutable_info());

    ++num_elements;
  }

  void add_way(const current_way &w) {
    check_overflow(element_WAY);

    current_way = pgroup->add_ways();
    current_way->set_id(w.id);
    set_info(w, current_way->mutable_info());

    m_last_way_node_ref = 0;
    ++num_elements;
  }

  void add_relation(const current_relation &r) {
    check_overflow(element_RELATION);

    current_relation = pgroup->add_relations();
    current_relation->set_id(r.id);
    set_info(r, current_relation->mutable_info());

    m_last_relation_member_ref = 0;
    ++num_elements;
  }

  void add_tag(const current_tag &t) {
    if (m_current_element == element_NULL) {
      throw std::runtime_error("Tag for NULL element type.");

    } else if (m_current_element == element_CHANGESET) {
      // OSMPBF brokenness - do nothing here.

    } else if (m_current_element == element_NODE) {
      if (current_node == NULL) { throw std::runtime_error("Tag before node? oops."); }
      current_node->add_keys(str_table(t.key));
      current_node->add_vals(str_table(t.value));

    } else if (m_current_element == element_WAY) {
      if (current_way == NULL) { throw std::runtime_error("Tag before way? oops."); }
      current_way->add_keys(str_table(t.key));
      current_way->add_vals(str_table(t.value));

    } else if (m_current_element == element_RELATION) {
      if (current_relation == NULL) { throw std::runtime_error("Tag before relation? oops."); }
      current_relation->add_keys(str_table(t.key));
      current_relation->add_vals(str_table(t.value));
    }
  }

  void add_way_node(const current_way_node &wn) {
    if (m_current_element != element_WAY) { throw std::runtime_error("Unexpected way node."); }
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
  }

  void add_relation_member(const current_relation_member &rm) {
    if (m_current_element != element_RELATION) { throw std::runtime_error("Unexpected relation member."); }
    current_relation->add_roles_sid(str_table(rm.member_role));
    current_relation->add_memids(int64_t(rm.member_id) - m_last_relation_member_ref);
    current_relation->add_types(member_type(rm.member_type));
    m_last_relation_member_ref = rm.member_id;
  }
  
  size_t num_elements;
  std::ostringstream buffer;
  std::ostream &out;
  string_table str_table;
  OSMPBF::PrimitiveBlock pblock;
  OSMPBF::PrimitiveGroup *pgroup;
  OSMPBF::Node *current_node;
  OSMPBF::Way *current_way;
  OSMPBF::Relation *current_relation;
  const size_t m_byte_limit;
  element_type m_current_element;
  int64_t m_last_way_node_ref, m_last_relation_member_ref;
};

pbf_writer::pbf_writer(std::ostream &out, const user_map_t &users, 
                       const bt::ptime &now)
  : m_impl(new pimpl(out, now)), m_users(users) {
}

pbf_writer::~pbf_writer() {
}

void pbf_writer::begin(const changeset &cs) {
  m_impl->add_changeset(cs);
}

void pbf_writer::begin(const current_node &n) {
  m_impl->add_node(n);
}

void pbf_writer::begin(const current_way &w) {
  m_impl->add_way(w);
}

void pbf_writer::begin(const current_relation &r) {
  m_impl->add_relation(r);
}

void pbf_writer::add(const current_tag &t) {
  m_impl->add_tag(t);
}

void pbf_writer::add(const current_way_node &wn) {
  m_impl->add_way_node(wn);
}

void pbf_writer::add(const current_relation_member &rm) {
  m_impl->add_relation_member(rm);
}

void pbf_writer::end() {
}

