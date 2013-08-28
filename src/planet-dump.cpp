#include <string>
#include <stdexcept>
#include <iostream>
#include <boost/format.hpp>
#include <boost/noncopyable.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <boost/filesystem.hpp>

#include <leveldb/db.h>
#include <leveldb/options.h>

#include "table_extractor.hpp"
#include "types.hpp"
#include "insert_kv.hpp"
#include "xml_writer.hpp"

namespace bt = boost::posix_time;
namespace fs = boost::filesystem;

template <typename T>
std::string pr_optional(const boost::optional<T> &t) {
  if (t) {
    T tt = t.get();
    return (boost::format("%1%") % tt).str();
  } else {
    return "<none>";
  }
}


template <typename R>
void extract_table(const std::string &table_name, 
                   const std::string &dump_file) {
  typedef R row_type;
  table_extractor<row_type> extractor(table_name, dump_file);
  extractor.read();
}

std::ostream &operator<<(std::ostream &out, const changeset &cs) {
  out << "changeset(" << cs.id << ", " << cs.uid << ", " << cs.created_at
      << ", " << pr_optional(cs.min_lon) << ", " << pr_optional(cs.min_lat)
      << ", " << pr_optional(cs.max_lon) << ", " << pr_optional(cs.max_lat)
      << ", " << cs.closed_at << ", " << cs.num_changes
      << ")";
  return out;
}

std::ostream &operator<<(std::ostream &out, const current_tag &t) {
  out << "current_tag(" << t.element_id
      << ", " << t.key
      << ", " << t.value
      << ")";
  return out;
}

std::ostream &operator<<(std::ostream &out, const current_node &n) {
  out << "current_node(" << n.id 
      << ", " << n.latitude
      << ", " << n.longitude
      << ", " << n.changeset_id
      << ", " << n.visible
      << ", " << n.timestamp
      << ", " << n.tile
      << ", " << n.version
      << ")";
  return out;
}

std::ostream &operator<<(std::ostream &out, const current_way &w) {
  out << "current_way(" << w.id
      << ", " << w.changeset_id
      << ", " << w.timestamp
      << ", " << w.visible
      << ", " << w.version
      << ")";
  return out;
}

std::ostream &operator<<(std::ostream &out, const current_way_node &wn) {
  out << "current_way_node(" << wn.way_id
      << ", " << wn.node_id
      << ", " << wn.sequence_id
      << ")";
  return out;
}

std::ostream &operator<<(std::ostream &out, const current_relation &r) {
  out << "current_relation(" << r.id
      << ", " << r.changeset_id
      << ", " << r.timestamp
      << ", " << r.visible
      << ", " << r.version
      << ")";
  return out;
}

std::ostream &operator<<(std::ostream &out, const current_relation_member &rm) {
  out << "current_relation_member(" << rm.relation_id
      << ", " << rm.member_type
      << ", " << rm.member_id
      << ", " << rm.member_role
      << ", " << rm.sequence_id
      << ")";
  return out;
}

template <typename T>
struct db_reader {
  db_reader(const std::string &name) : m_db(NULL), m_itr(NULL) {
    m_options.create_if_missing = false;
    m_options.error_if_exists = false;
    
    leveldb::Status status;
    status = leveldb::DB::Open(m_options, name, &m_db);
    if (!status.ok()) {
      throw std::runtime_error((boost::format("Can't open database: %1%") % status.ToString()).str());
    }

    m_itr = m_db->NewIterator(m_read_options);
    m_itr->SeekToFirst();
  }

  ~db_reader() {
    if (m_itr != NULL) {
      delete m_itr;
    }
    if (m_db != NULL) {
      delete m_db;
    }
  }

  bool operator()(T &t) {
    const bool valid = m_itr->Valid();
    if (valid) {
      leveldb::Slice key = m_itr->key();
      leveldb::Slice val = m_itr->value();
      insert_kv(t, key, val);
      m_itr->Next();
    }
    return valid;
  }
  
  leveldb::DB *m_db;
  leveldb::Iterator *m_itr;
  leveldb::Options m_options;
  leveldb::ReadOptions m_read_options;
};

void extract_users(const std::string &dump_file, std::map<int64_t, std::string> &display_name_map) {
  if (!fs::exists("users")) {
    extract_table<user>("users", dump_file);
  }

  db_reader<user> reader("users");
  user u;
  display_name_map.clear();
  while (reader(u)) {
    if (u.data_public) {
      display_name_map.insert(std::make_pair(u.id, u.display_name));
    }
  }
}

void extract_changesets(const std::string &dump_file, xml_writer &writer) {
  if (!fs::exists("changesets")) {
    extract_table<changeset>("changesets", dump_file);
  }
  if (!fs::exists("changeset_tags")) {
    extract_table<current_tag>("changeset_tags", dump_file);
  }

  db_reader<changeset> cs_reader("changesets");
  db_reader<current_tag> cst_reader("changeset_tags");
  changeset cs;
  current_tag cst;
  cst.element_id = 0;
  while (cs_reader(cs)) {
    writer.begin(cs);
    
    while (cst.element_id <= cs.id) {
      if (cst.element_id == cs.id) {
        writer.add(cst);
      }
      if (!cst_reader(cst)) {
        break;
      }
    }

    writer.end();
  }
}

void extract_current_nodes(const std::string &dump_file, xml_writer &writer) {
  if (!fs::exists("current_nodes")) {
    extract_table<current_node>("current_nodes", dump_file);
  }
  if (!fs::exists("current_node_tags")) {
    extract_table<current_tag>("current_node_tags", dump_file);
  }

  db_reader<current_node> n_reader("current_nodes");
  db_reader<current_tag> nt_reader("current_node_tags");
  current_node n;
  current_tag nt;
  nt.element_id = 0;
  while (n_reader(n)) {
    writer.begin(n);
    
    while (nt.element_id <= n.id) {
      if (nt.element_id == n.id) {
        writer.add(nt);
      }
      if (!nt_reader(nt)) {
        break;
      }
    }

    writer.end();
  }
}

void extract_current_ways(const std::string &dump_file, xml_writer &writer) {
  if (!fs::exists("current_ways")) {
    extract_table<current_way>("current_ways", dump_file);
  }
  if (!fs::exists("current_way_tags")) {
    extract_table<current_tag>("current_way_tags", dump_file);
  }
  if (!fs::exists("current_way_nodes")) {
    extract_table<current_way_node>("current_way_nodes", dump_file);
  }

  db_reader<current_way> w_reader("current_ways");
  db_reader<current_tag> wt_reader("current_way_tags");
  db_reader<current_way_node> wn_reader("current_way_nodes");
  current_way w;
  current_tag wt;
  current_way_node wn;
  wt.element_id = 0;
  wn.way_id = 0;
  while (w_reader(w)) {
    writer.begin(w);
    
    while (wn.way_id <= w.id) {
      if (wn.way_id == w.id) {
        writer.add(wn);
      }
      if (!wn_reader(wn)) {
        break;
      }
    }

    while (wt.element_id <= w.id) {
      if (wt.element_id == w.id) {
        writer.add(wt);
      }
      if (!wt_reader(wt)) {
        break;
      }
    }

    writer.end();
  }
}

void extract_current_relations(const std::string &dump_file, xml_writer &writer) {
  if (!fs::exists("current_relations")) {
    extract_table<current_relation>("current_relations", dump_file);
  }
  if (!fs::exists("current_relation_tags")) {
    extract_table<current_tag>("current_relation_tags", dump_file);
  }
  if (!fs::exists("current_relation_members")) {
    extract_table<current_relation_member>("current_relation_members", dump_file);
  }

  db_reader<current_relation> r_reader("current_relations");
  db_reader<current_tag> rt_reader("current_relation_tags");
  db_reader<current_relation_member> rm_reader("current_relation_members");
  current_relation r;
  current_tag rt;
  current_relation_member rm;
  rt.element_id = 0;
  rm.relation_id = 0;
  while (r_reader(r)) {
    writer.begin(r);
    
    while (rm.relation_id <= r.id) {
      if (rm.relation_id == r.id) {
        writer.add(rm);
      }
      if (!rm_reader(rm)) {
        break;
      }
    }

    while (rt.element_id <= r.id) {
      if (rt.element_id == r.id) {
        writer.add(rt);
      }
      if (!rt_reader(rt)) {
        break;
      }
    }

    writer.end();
  }
}

int main(int argc, char *argv[]) {
  try {
    if (argc != 2) {
       throw std::runtime_error("Usage: ./planet-dump <file>");
    }
    std::string dump_file(argv[1]);
    std::map<int64_t, std::string> display_name_map;
    extract_users(dump_file, display_name_map);
    xml_writer writer(std::cout, display_name_map);
    extract_changesets(dump_file, writer);
    extract_current_nodes(dump_file, writer);
    //extract_current_ways(dump_file, writer);
    //extract_current_relations(dump_file, writer);

  } catch (const std::exception &e) {
    std::cerr << "EXCEPTION: " << e.what() << "\n";
    return 1;

  } catch (...) {
    std::cerr << "UNEXPLAINED ERROR\n";
    return 1;
  }

  return 0;
}
