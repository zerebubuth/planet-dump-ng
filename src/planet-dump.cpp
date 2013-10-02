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

struct dual_writer {
  dual_writer(pbf_writer &pwriter, xml_writer &xwriter) 
    : m_pwriter(pwriter), m_xwriter(xwriter) {
  }

  void begin(const changeset &c)        { m_pwriter.begin(c); m_xwriter.begin(c); }
  void begin(const current_node &n)     { m_pwriter.begin(n); m_xwriter.begin(n); }
  void begin(const current_way &w)      { m_pwriter.begin(w); m_xwriter.begin(w); }
  void begin(const current_relation &r) { m_pwriter.begin(r); m_xwriter.begin(r); }

  void add(const current_tag &t)              { m_pwriter.add(t); m_xwriter.add(t); }
  void add(const current_way_node &wn)        { m_pwriter.add(wn); m_xwriter.add(wn); }
  void add(const current_relation_member &rm) { m_pwriter.add(rm); m_xwriter.add(rm); }

  void end() { m_pwriter.end(); m_xwriter.end(); }

  pbf_writer &m_pwriter;
  xml_writer &m_xwriter;
};

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

template <>
struct db_reader<int> {
  db_reader(const std::string &) {}
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

void extract_changesets(const std::string &dump_file, dual_writer &writer) {
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

template <typename T> void zero_init(T &);
template <typename T> int64_t id_of(const T &);

template <> inline void zero_init<current_tag>(current_tag &t) { t.element_id = 0; }
template <> inline void zero_init<current_way_node>(current_way_node &wn) { wn.way_id = 0; }
template <> inline void zero_init<current_relation_member>(current_relation_member &rm) { rm.relation_id = 0; }
template <> inline void zero_init<int>(int &) { }

template <> inline int64_t id_of<current_tag>(const current_tag &t) { return t.element_id; }
template <> inline int64_t id_of<current_way_node>(const current_way_node &wn) { return wn.way_id; }
template <> inline int64_t id_of<current_relation_member>(const current_relation_member &rm) { return rm.relation_id; }

template <typename T>
inline void fetch_associated(T &t, int64_t id, db_reader<T> &reader, dual_writer &writer) {
  while (id_of<T>(t) <= id) {
    if (id_of<T>(t) == id) {
      writer.add(t);
    }
    if (!reader(t)) {
      break;
    }
  }
}

template <>
inline void fetch_associated<int>(int &, int64_t, db_reader<int> &, dual_writer &) {
}

template <typename T>
void extract_element(const std::string &dump_file, dual_writer &writer) {
  typedef typename T::tag_type tag_type;
  typedef typename T::inner_type inner_type;

  db_reader<T> element_reader(T::table_name());
  db_reader<tag_type> tag_reader(T::tag_table_name());
  db_reader<inner_type> inner_reader(T::inner_table_name());

  T current_element;
  tag_type current_tag;
  inner_type current_inner;

  zero_init<tag_type>(current_tag);
  zero_init<inner_type>(current_inner);

  while (element_reader(current_element)) {
    if (!current_element.visible) { continue; }

    writer.begin(current_element);
    
    fetch_associated(current_inner, current_element.id, inner_reader, writer);
    fetch_associated(current_tag, current_element.id, tag_reader, writer);

    writer.end();
  }
}

void extract_current_nodes(const std::string &dump_file, dual_writer &writer) {
  extract_element<current_node>(dump_file, writer);
}

void extract_current_ways(const std::string &dump_file, dual_writer &writer) {
  extract_element<current_way>(dump_file, writer);
}

void extract_current_relations(const std::string &dump_file, dual_writer &writer) {
  extract_element<current_relation>(dump_file, writer);
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
    if (argc != 3) {
       throw std::runtime_error("Usage: ./planet-dump <file> <pbf-file>");
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
    std::ofstream pbf_out(argv[2]);
    pbf_writer pwriter(pbf_out, display_name_map, max_time);
    xml_writer xwriter(std::cout, display_name_map, max_time);
    dual_writer writer(pwriter, xwriter);

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
