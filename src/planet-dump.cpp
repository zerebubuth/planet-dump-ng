#include <string>
#include <stdexcept>
#include <iostream>
#include <fstream>

#include <boost/format.hpp>
#include <boost/noncopyable.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/exception/all.hpp>
#include <boost/thread.hpp>
#include <boost/make_shared.hpp>
#include <boost/foreach.hpp>

#include <leveldb/db.h>
#include <leveldb/options.h>

#include "table_extractor.hpp"
#include "types.hpp"
#include "insert_kv.hpp"
#include "pbf_writer.hpp"

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
  fs::path base_dir(table_name);
  bool needs_dump = true;

  if (fs::exists(base_dir)) {
    if (fs::is_directory(base_dir) && fs::exists(base_dir / ".complete")) {
      needs_dump = false;

    } else {
      fs::remove_all(base_dir);
    }
  }

  if (needs_dump) {
    table_extractor<row_type> extractor(table_name, dump_file);
    extractor.read();
    fs::ofstream out(base_dir / ".complete");
    out << "\n";
  }
}

template <typename R>
bt::ptime extract_table_with_timestamp(const std::string &table_name, 
                                       const std::string &dump_file) {
  typedef R row_type;
  fs::path base_dir(table_name);
  boost::optional<bt::ptime> timestamp;

  if (fs::exists(base_dir)) {
    if (fs::is_directory(base_dir) && fs::exists(base_dir / ".complete")) {
      std::string timestamp_str;
      fs::ifstream in(base_dir / ".complete");
      std::getline(in, timestamp_str);
      if (timestamp_str == "-infinity") {
        timestamp = bt::ptime(bt::neg_infin);
      } else {
        timestamp = bt::time_from_string(timestamp_str);
      }

    } else {
      fs::remove_all(base_dir);
    }
  }

  if (timestamp) {
    return timestamp.get();

  } else {
    table_extractor_with_timestamp<row_type> extractor(table_name, dump_file);
    timestamp = extractor.read();
    fs::ofstream out(base_dir / ".complete");
    out << bt::to_simple_string(timestamp.get()) << "\n";
    return timestamp.get();
  }
}

template <typename R>
void thread_extract_with_timestamp(bt::ptime &timestamp,
                                   boost::exception_ptr &error,
                                   std::string table_name,
                                   std::string dump_file) {
  try {
    bt::ptime ts = extract_table_with_timestamp<R>(table_name, dump_file);
    timestamp = ts;

  } catch (const boost::exception &e) {
    error = boost::current_exception();

  } catch (const std::exception &e) {
    error = boost::current_exception();

  } catch (...) {
    std::cerr << "Unexpected exception of unknown type in "
              << "thread_extract_with_timestamp(" << table_name 
              << ", " << dump_file << ")!" << std::endl;
    abort();
  }
}

std::ostream &operator<<(std::ostream &out, const changeset &cs) {
  out << "changeset(" << cs.id << ", " << cs.uid << ", " << cs.created_at
      << ", " << pr_optional(cs.min_lon) << ", " << pr_optional(cs.min_lat)
      << ", " << pr_optional(cs.max_lon) << ", " << pr_optional(cs.max_lat)
      << ", " << cs.closed_at
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
  extract_table<user>("users", dump_file);

  db_reader<user> reader("users");
  user u;
  display_name_map.clear();
  while (reader(u)) {
    if (u.data_public) {
      display_name_map.insert(std::make_pair(u.id, u.display_name));
    }
  }
}

void extract_changesets(const std::string &dump_file, pbf_writer &writer) {
  extract_table<changeset>("changesets", dump_file);
  extract_table<current_tag>("changeset_tags", dump_file);

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

void extract_current_nodes(const std::string &dump_file, pbf_writer &writer) {
  extract_table<current_tag>("current_node_tags", dump_file);

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

void extract_current_ways(const std::string &dump_file, pbf_writer &writer) {
  extract_table<current_tag>("current_way_tags", dump_file);
  extract_table<current_way_node>("current_way_nodes", dump_file);

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

void extract_current_relations(const std::string &dump_file, pbf_writer &writer) {
  extract_table<current_tag>("current_relation_tags", dump_file);
  extract_table<current_relation_member>("current_relation_members", dump_file);

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

struct base_thread {
  virtual ~base_thread() {}
  virtual bt::ptime join() = 0;
};

template <typename R>
struct run_thread : public base_thread {
  bt::ptime timestamp;
  boost::exception_ptr error;
  boost::thread thr;

  run_thread(std::string table_name, std::string dump_file)
    : timestamp(), error(), thr(&thread_extract_with_timestamp<R>,
                                boost::ref(timestamp), boost::ref(error),
                                table_name, dump_file) {
  }

  ~run_thread() {
    try {
      thr.join();
    } catch (...) {
    }
  }

  bt::ptime join() {
    thr.join();
    if (error) {
      boost::rethrow_exception(error);
    }
    return timestamp;
  }
};

int main(int argc, char *argv[]) {
  try {
    if (argc != 2) {
       throw std::runtime_error("Usage: ./planet-dump <file>");
    }

    // workaround for https://svn.boost.org/trac/boost/ticket/5638
    boost::gregorian::greg_month::get_month_map_ptr();

    std::string dump_file(argv[1]);
    std::map<int64_t, std::string> display_name_map;

    std::list<boost::shared_ptr<base_thread> > threads;

    threads.push_back(boost::make_shared<run_thread<changeset> >("changesets", dump_file));
    threads.push_back(boost::make_shared<run_thread<current_node> >("current_nodes", dump_file));
    threads.push_back(boost::make_shared<run_thread<current_way> >("current_ways", dump_file));
    threads.push_back(boost::make_shared<run_thread<current_relation> >("current_relations", dump_file));

    threads.push_back(boost::make_shared<run_thread<current_tag> >("current_node_tags", dump_file));
    threads.push_back(boost::make_shared<run_thread<current_tag> >("current_way_tags", dump_file));
    threads.push_back(boost::make_shared<run_thread<current_tag> >("current_relation_tags", dump_file));
    threads.push_back(boost::make_shared<run_thread<current_way_node> >("current_way_nodes", dump_file));
    threads.push_back(boost::make_shared<run_thread<current_relation_member> >("current_relation_members", dump_file));

    threads.push_back(boost::make_shared<run_thread<user> >("users", dump_file));

    bt::ptime max_time(bt::neg_infin);
    BOOST_FOREACH(boost::shared_ptr<base_thread> &thr, threads) {
      max_time = std::max(max_time, thr->join());
      thr.reset();
    }
    threads.clear();

    extract_users(dump_file, display_name_map);
    pbf_writer writer(std::cout, display_name_map, max_time);
    std::cerr << "Writing changesets..." << std::endl;
    extract_changesets(dump_file, writer);
    std::cerr << "Writing nodes..." << std::endl;
    extract_current_nodes(dump_file, writer);
    std::cerr << "Writing ways..." << std::endl;
    extract_current_ways(dump_file, writer);
    std::cerr << "Writing relations..." << std::endl;
    extract_current_relations(dump_file, writer);
    std::cerr << "Done" << std::endl;

  } catch (const boost::exception &e) {
    std::cerr << "EXCEPTION: " << boost::current_exception_diagnostic_information() << "\n";
    return 1;

  } catch (const std::exception &e) {
    std::cerr << "EXCEPTION: " << boost::current_exception_diagnostic_information() << "\n";
    return 1;

  } catch (...) {
    std::cerr << "UNEXPLAINED ERROR\n";
    return 1;
  }

  return 0;
}
