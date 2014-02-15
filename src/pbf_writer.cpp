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

#define ASSERT_EQ(a, b) { if ((a) != (b)) {		    \
      std::ostringstream out;				    \
      out << "Assertion " #a " == " #b " failed, ";	    \
      out << (a) << " != " << (b) << ".";		    \
      BOOST_THROW_EXCEPTION(std::runtime_error(out.str())); \
    } }
#define ASSERT_NOT_NULL(a) { if ((a) == NULL) {		    \
      std::ostringstream out;				    \
      out << "Assertion " #a " != NULL failed, ";	    \
      out << (a) << " is NULL.";			    \
      BOOST_THROW_EXCEPTION(std::runtime_error(out.str())); \
    } }

namespace bt = boost::posix_time;

namespace {

struct string_table {
  typedef boost::unordered_map<std::string, int> string_map_t;

  string_table() : m_strings(), m_indexed_strings(), m_next_id(1), m_approx_size(0) {}

  int operator()(const std::string &s) {
    string_map_t::iterator itr = m_strings.find(s);
    if (itr == m_strings.end()) {
      int key = m_next_id;
      ++m_next_id;
      m_strings.insert(std::make_pair(s, key));
      m_indexed_strings.push_back(s);
      // keep track of approximate size - strings are stored as the bytes of
      // the string, length prefixed with varint which should be 1 byte if
      // the value is < 128, and more if it's more. OSM strings are generally
      // limited to 255 unicode chars, so unlikely to be > 16k bytes.
      m_approx_size += s.size() + (s.size() > 128 ? 2 : 1);
      return key;

    } else {
      return itr->second;
    }
  }

  size_t approx_size() const { return m_approx_size; }

  void clear() {
    m_strings.clear();
    m_indexed_strings.clear();
    m_next_id = 1;
    m_approx_size = 0;
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
  size_t m_approx_size;
};

// simple function to calculate the delta between the last value of
// something and the current value, returning the difference and
// updating the last value.
template <typename T>
inline T delta(T &last, T cur) {
  T d = cur - last;
  last = cur;
  return d;
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

  pimpl(const std::string &out_name, const bt::ptime &now, bool history_format,
        const user_map_t &user_map, const boost::program_options::variables_map &options) 
    : num_elements(0), buffer(), out(out_name.c_str()), str_table(),
      pblock(), m_dense_section(), pgroup(pblock.add_primitivegroup()), 
      current_node(NULL), current_way(NULL), current_relation(NULL),
      m_byte_limit(int(0.25 * OSMPBF::max_uncompressed_blob_size)),
      m_current_element(element_NULL),
      m_last_way_node_ref(0),
      m_last_relation_member_ref(0),
      m_est_pblock_size(0),
      m_history_format(history_format),
      m_user_map(user_map),
      m_changeset_user_map(),
      m_recheck_elements(int(element_RELATION) + 1) {

    // different re-check limits per type so that we can better
    // adapt to the different sizes of elements, and hit the
    // byte limit without overflowing it.
    m_recheck_elements[element_NULL] = 1;
    m_recheck_elements[element_CHANGESET] = 1;
    m_recheck_elements[element_NODE] = 16000;
    m_recheck_elements[element_WAY] = 8000;
    m_recheck_elements[element_RELATION] = 2000;

    m_dense_nodes = options["dense-nodes"].as<bool>();

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
    if (m_dense_nodes) {
      header.add_required_features("DenseNodes");
    }
    header.add_optional_features("Has_Metadata");
    header.add_optional_features("Sort.Type_then_ID");
    header.set_writingprogram(PACKAGE_STRING);
    header.set_source(OSM_API_ORIGIN);
#ifndef WITH_OLD_OSMPBF
    header.set_osmosis_replication_timestamp((now - bt::from_time_t(time_t(0))).total_seconds());
#endif

    write_blob(header, "OSMHeader");
  }

  void write_blob(const google::protobuf::MessageLite &message, const std::string &type) {
    using namespace OSMPBF;
    using google::protobuf::io::StringOutputStream;
    using google::protobuf::io::GzipOutputStream;

    Blob blob;
    size_t uncompressed_size = message.ByteSize();
    // sanity check - if we're about to violate the OSMPBF format rules
    // then we'd rather stop than ship an invalid file.
    if (uncompressed_size >= OSMPBF::max_uncompressed_blob_size) {
      std::ostringstream ostr;
      ostr << "Unable to write block of type " << type << ", uncompressed size " << uncompressed_size
           << " because it is larger than the maximum allowed " << OSMPBF::max_uncompressed_blob_size
           << "." << std::endl;
      BOOST_THROW_EXCEPTION(std::runtime_error(ostr.str()));
    }
    blob.set_raw_size(uncompressed_size);

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

    int blob_header_size = blob_header.ByteSize();
    if (blob_header_size < 0) {
      std::ostringstream ostr;
      ostr << "Unable to write blob header size " << blob_header_size 
           << " because it will not correctly cast to uint32_t.";
      BOOST_THROW_EXCEPTION(std::runtime_error(ostr.str()));
    }
    uint32_t bh_size = htonl(uint32_t(blob_header_size));
    out.write((char *)&bh_size, sizeof bh_size);
    out << blob_header.SerializeAsString();
    out << blob.SerializeAsString();
    out.flush();
  }

  void check_overflow(element_type type) {
    if ((m_current_element == element_NULL) ||
        (m_current_element == element_CHANGESET)) {  // <- to deal with OSMPBF brokenness...
      m_current_element = type;
    }

    if ((m_current_element != type) || 
        (num_elements >= m_recheck_elements[m_current_element])) {
      m_est_pblock_size += pgroup->ByteSize();
      bool new_block = ((m_current_element != type) || 
                        ((m_est_pblock_size + str_table.approx_size()) >= m_byte_limit));

      if (new_block) {
        str_table.write(pblock.mutable_stringtable());

	// before writing, check that, if we have dense nodes, we have the
	// same number of entries for all the arrays.
	if (m_dense_nodes && (m_dense_section != NULL)) {
	  check_dense_node_arrays();
	}

        write_blob(pblock, "OSMData");
        pblock.Clear();
        str_table.clear();
        
        m_current_element = type;
        m_est_pblock_size = 0;
      }

      m_last_dense_id = 0;
      m_last_dense_lat = 0;
      m_last_dense_lon = 0;
      m_last_dense_timestamp = 0;
      m_last_dense_changeset = 0;
      m_last_dense_uid = 0;
      m_last_dense_user_sid = 0;
      pgroup = pblock.add_primitivegroup();
      num_elements = 0;
      current_node = NULL;
      current_way = NULL;
      current_relation = NULL;
    }
  }

  void check_dense_node_arrays() {
    ASSERT_EQ(m_dense_nodes, true);
    ASSERT_NOT_NULL(m_dense_section);

    OSMPBF::DenseInfo *info = m_dense_section->mutable_denseinfo();
    assert(info != NULL);

    int num_ids = m_dense_section->id_size();
    int num_lons = m_dense_section->lon_size();
    int num_lats = m_dense_section->lat_size();
    int num_versions = info->version_size();
    int num_timestamps = info->timestamp_size();
    int num_changesets = info->changeset_size();
    int num_visibles = info->visible_size();
    int num_uids = info->uid_size();
    int num_user_sids = info->user_sid_size();

    ASSERT_EQ(num_ids, num_lons);
    ASSERT_EQ(num_ids, num_lats);
    ASSERT_EQ(num_ids, num_versions);
    ASSERT_EQ(num_ids, num_timestamps);
    ASSERT_EQ(num_ids, num_changesets);
    ASSERT_EQ(num_ids, num_visibles);
    ASSERT_EQ(num_ids, num_uids);
    ASSERT_EQ(num_ids, num_user_sids);
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
      // for anonymous, just leave the uid & user_sid blank.
    } else {
      std::ostringstream out;
      out << "Unable to find changeset " << t.changeset_id 
	  << " in changeset-to-user map.";
      BOOST_THROW_EXCEPTION(std::runtime_error(out.str()));
    }
  }

  void add_changeset(const changeset &cs) {
    // looks like OSMPBF is broken and doesn't really support this.
    check_overflow(element_CHANGESET);
  }

  void add_node(const node &n) {
    check_overflow(element_NODE);
    if (m_dense_nodes) return add_dense_node(n);

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

  void add_dense_node(const node &n) {
    static bt::ptime epoch = bt::from_time_t(time_t(0));
    current_node = NULL;
    m_dense_section = pgroup->mutable_dense();
    m_dense_section->add_id(delta(m_last_dense_id, n.id));
    m_dense_section->add_lon(delta(m_last_dense_lon, int64_t(n.visible ? n.longitude : 0)));
    m_dense_section->add_lat(delta(m_last_dense_lat, int64_t(n.visible ? n.latitude : 0)));
    OSMPBF::DenseInfo* info = m_dense_section->mutable_denseinfo();
    info->add_version(n.version);
    info->add_timestamp(delta(m_last_dense_timestamp, int64_t((n.timestamp - epoch).total_seconds())));
    info->add_changeset(delta(m_last_dense_changeset, n.changeset_id));
    // if we are doing a history file, we need to set the visible flag
    // for all entries in the dense node table, as this array is indexed
    // into by position to get the visibility flag.
    if (m_history_format) {
      info->add_visible(n.visible);
    }
    // set the uid and user information, if the user is public
    std::map<int64_t, int64_t>::const_iterator itr = m_changeset_user_map.find(n.changeset_id);
    if (itr != m_changeset_user_map.end()) {
      user_map_t::const_iterator jtr = m_user_map.find(itr->second);
      if (jtr != m_user_map.end()) {
        info->add_uid(delta(m_last_dense_uid, jtr->first));
        info->add_user_sid(delta(m_last_dense_user_sid, str_table(jtr->second)));
      }
      else
      {
        // anonymous
        info->add_uid(delta(m_last_dense_uid, int64_t(-1)));
        info->add_user_sid(delta(m_last_dense_user_sid, str_table("")));
      }
    } else {
      std::ostringstream out;
      out << "Unable to find changeset " << n.changeset_id 
	  << " in changeset-to-user map for dense node.";
      BOOST_THROW_EXCEPTION(std::runtime_error(out.str()));
    }
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

  void add_dense_tag(const old_tag &t) {
    if (m_dense_section== NULL) {
      BOOST_THROW_EXCEPTION(std::runtime_error("No dense section available for tag."));
    }
    m_dense_section->add_keys_vals(str_table(t.key));
    m_dense_section->add_keys_vals(str_table(t.value));
  }

  void add_node_finish() {
    if (m_dense_nodes) m_dense_section->add_keys_vals(0);
  }

  void add_tag(const old_tag &t, bool node_section) {
    if (m_dense_nodes && node_section) return add_dense_tag(t);
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

    BOOST_THROW_EXCEPTION(std::runtime_error("Unknown nwr_enum value in member_type."));
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
  bool m_dense_nodes;
  OSMPBF::DenseNodes* m_dense_section;
  std::map<int64_t, int64_t> m_changeset_user_map;
  std::vector<size_t> m_recheck_elements;

  int64_t m_last_dense_id;
  int64_t m_last_dense_lat;
  int64_t m_last_dense_lon;
  int64_t m_last_dense_timestamp;
  int64_t m_last_dense_changeset;
  int64_t m_last_dense_uid;
  int m_last_dense_user_sid;



private:
  
  pimpl(const pimpl &);
  const pimpl &operator=(const pimpl &);
};

pbf_writer::pbf_writer(const std::string &file_name, const boost::program_options::variables_map &options, 
                       const user_map_t &users, const boost::posix_time::ptime &now, bool history_format)
  : m_impl(new pimpl(file_name, now, history_format, users, options)) {
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

    if (n.visible) {
      while ((tag_itr != ts.end()) && 
             ((tag_itr->element_id < n.id) ||
              ((tag_itr->element_id == n.id) &&
               (tag_itr->version <= n.version)))) {
        if ((tag_itr->element_id == n.id) && (tag_itr->version == n.version)) {
          m_impl->add_tag(*tag_itr, true);
        }
        ++tag_itr;
      }
    }
    m_impl->add_node_finish();
  }    
}

void pbf_writer::ways(const std::vector<way> &ws,
                      const std::vector<way_node> &wns,
                      const std::vector<old_tag> &ts) {
  std::vector<old_tag>::const_iterator tag_itr = ts.begin();
  std::vector<way_node>::const_iterator nd_itr = wns.begin();

  BOOST_FOREACH(const way &w, ws) {
    m_impl->add_way(w);

    if (!w.visible) { continue; }

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
        m_impl->add_tag(*tag_itr, false);
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

    if (!r.visible) { continue; }

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
        m_impl->add_tag(*tag_itr, false);
      }
      ++tag_itr;
    }
  }
}

void pbf_writer::finish() {
  m_impl->finish();
}
