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

template <typename T>
struct control_block {
  typedef typename T::tag_type tag_type;
  typedef typename T::inner_type inner_type;

  control_block(unsigned int num_threads)
  : pre_swap_barrier(num_threads),
    post_swap_barrier(num_threads),
    thread_finished_index(-1) {
  }

  boost::barrier pre_swap_barrier, post_swap_barrier;

  int thread_finished_index;
  boost::mutex thread_finished_mutex;
  boost::condition_variable thread_finished_cond;

  std::vector<T> elements;
  std::vector<tag_type> tags;
  std::vector<inner_type> inners;
};

template <typename T>
struct thread_writer {
  typedef typename T::tag_type tag_type;
  typedef typename T::inner_type inner_type;

  boost::shared_ptr<control_block<T> > blk;

  thread_writer(boost::shared_ptr<control_block<T> > b) : blk(b) {}

  void write(std::vector<T> &els, std::vector<inner_type> &inners, std::vector<tag_type> &tags) {
    std::cerr.write("Got block for write\n", 20); std::cerr.flush();
    blk->pre_swap_barrier.wait();
    std::swap(els, blk->elements);
    std::swap(inners, blk->inners);
    std::swap(tags, blk->tags);
    blk->post_swap_barrier.wait();
  }
};

#define BLOCK_SIZE (1048576)

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
inline void fetch_associated(T &t, int64_t id, db_reader<T> &reader, std::vector<T> &vec) {
  while (id_of<T>(t) <= id) {
    if (id_of<T>(t) == id) {
      vec.push_back(t);
    }
    if (!reader(t)) {
      break;
    }
  }
}

template <>
inline void fetch_associated<int>(int &, int64_t, db_reader<int> &, std::vector<int> &) {
}

template <typename T>
void extract_element(const std::string &dump_file, thread_writer<T> &writer) {
  typedef typename T::tag_type tag_type;
  typedef typename T::inner_type inner_type;

  db_reader<T> element_reader(T::table_name());
  db_reader<tag_type> tag_reader(T::tag_table_name());
  db_reader<inner_type> inner_reader(T::inner_table_name());

  std::vector<T> elements;
  std::vector<tag_type> tags;
  std::vector<inner_type> inners;
  
  elements.resize(BLOCK_SIZE);
  size_t i = 0;

  tag_type current_tag;
  inner_type current_inner;

  zero_init<tag_type>(current_tag);
  zero_init<inner_type>(current_inner);

  while (element_reader(elements[i])) {
    if (!elements[i].visible) { continue; }
    
    fetch_associated(current_inner, elements[i].id, inner_reader, inners);
    fetch_associated(current_tag, elements[i].id, tag_reader, tags);

    ++i;
    if (i == BLOCK_SIZE) {
      writer.write(elements, inners, tags);
      inners.clear();
      tags.clear();
      i = 0;
      if (elements.size() != BLOCK_SIZE) { elements.resize(BLOCK_SIZE); }
    }
  }

  elements.resize(i);
  writer.write(elements, inners, tags);
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

template <typename T>
void reader_thread(int thread_index, 
                   boost::exception_ptr exc, 
                   const std::string &dump_file, 
                   boost::shared_ptr<control_block<T> > blk) {
  try {
    thread_writer<T> writer(blk);
    extract_element<T>(dump_file, writer);

  } catch (...) {
    exc = boost::current_exception();
  }

  boost::lock_guard<boost::mutex> lock(blk->thread_finished_mutex);
  blk->thread_finished_index = thread_index;
  blk->thread_finished_cond.notify_one();
}

template <typename T> void write_elements(output_writer &writer, control_block<T> &blk);

template <> inline void write_elements<current_node>(output_writer &writer, control_block<current_node> &blk) { 
  writer.nodes(blk.elements, blk.tags);
}
template <> inline void write_elements<current_way>(output_writer &writer, control_block<current_way> &blk) { 
  writer.ways(blk.elements, blk.inners, blk.tags);
}
template <> inline void write_elements<current_relation>(output_writer &writer, control_block<current_relation> &blk) { 
  writer.relations(blk.elements, blk.inners, blk.tags);
}

template <typename T>
void writer_thread(int thread_index,
                   boost::exception_ptr exc,
                   boost::shared_ptr<output_writer> writer, 
                   boost::shared_ptr<control_block<T> > blk) {
  try {
    do {
      blk->pre_swap_barrier.wait();
      blk->post_swap_barrier.wait();
      
      std::cerr.write("Got block for read\n", 19); std::cerr.flush();
      write_elements<T>(*writer, *blk);
      
    } while (blk->elements.size() == BLOCK_SIZE);

  } catch (...) {
    exc = boost::current_exception();
  }

  boost::lock_guard<boost::mutex> lock(blk->thread_finished_mutex);
  blk->thread_finished_index = thread_index;
  blk->thread_finished_cond.notify_one();
}

void join_all_but(size_t i, std::vector<boost::shared_ptr<boost::thread> > &threads) {
  for (size_t j = 0; j < threads.size(); ++j) {
    if ((j != i) && threads[j]->joinable()) {
      threads[j]->join();
    }
  }
}

template <typename T>
void run_threads(const std::string &dump_file, 
                 std::vector<boost::shared_ptr<output_writer> > writers) {
  std::vector<boost::shared_ptr<boost::thread> > threads;
  std::vector<boost::exception_ptr> exceptions;
  const int num_threads = writers.size() + 1;
  int i = 0, num_running_threads = num_threads;

  exceptions.resize(num_threads);
  boost::shared_ptr<control_block<T> > blk = boost::make_shared<control_block<T> >(writers.size() + 1);

  threads.push_back(boost::make_shared<boost::thread>(boost::bind(&reader_thread<T>, i, exceptions[i], dump_file, blk)));

  BOOST_FOREACH(boost::shared_ptr<output_writer> writer, writers) {
    ++i;
    threads.push_back(boost::make_shared<boost::thread>(boost::bind(&writer_thread<T>, i, exceptions[i], writer, blk)));
  }

  {
    boost::unique_lock<boost::mutex> lock(blk->thread_finished_mutex);
    while (num_running_threads > 0) {
      blk->thread_finished_cond.wait(lock);
      int idx = blk->thread_finished_index;
      
      if (idx >= 0) {
        blk->thread_finished_index = -1;
        
        boost::shared_ptr<boost::thread> thread = threads[idx];
        thread->join();
        --num_running_threads;
        
        if (exceptions[idx]) {
          // interrupt all other threads and join them
          join_all_but(idx, threads);
          // re-throw the exception
          boost::rethrow_exception(exceptions[idx]);
        }
      }
    }
  }
}

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
    std::vector<boost::shared_ptr<output_writer> > writers;
    writers.push_back(boost::shared_ptr<output_writer>(new pbf_writer(pbf_out, display_name_map, max_time)));
    writers.push_back(boost::shared_ptr<output_writer>(new xml_writer(std::cout, display_name_map, max_time)));

    std::cerr << "Writing changesets..." << std::endl;
    //extract_changesets(dump_file, writer);
    std::cerr << "Writing nodes..." << std::endl;
    run_threads<current_node>(dump_file, writers);
    std::cerr << "Writing ways..." << std::endl;
    run_threads<current_way>(dump_file, writers);
    std::cerr << "Writing relations..." << std::endl;
    run_threads<current_relation>(dump_file, writers);
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
