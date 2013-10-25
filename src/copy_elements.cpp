#include "copy_elements.hpp"
#include "insert_kv.hpp"
#include "types.hpp"

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

namespace {

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

#define BLOCK_SIZE (1048576)

template <typename T> void zero_init(T &);
template <typename T> int64_t id_of(const T &);

template <> inline void zero_init<current_tag>(current_tag &t) { t.element_id = 0; }
template <> inline void zero_init<old_tag>(old_tag &t) { t.element_id = 0; }
template <> inline void zero_init<way_node>(way_node &wn) { wn.way_id = 0; }
template <> inline void zero_init<relation_member>(relation_member &rm) { rm.relation_id = 0; }
template <> inline void zero_init<int>(int &) { }

template <> inline int64_t id_of<current_tag>(const current_tag &t) { return t.element_id; }
template <> inline int64_t id_of<old_tag>(const old_tag &t) { return t.element_id; }
template <> inline int64_t id_of<way_node>(const way_node &wn) { return wn.way_id; }
template <> inline int64_t id_of<relation_member>(const relation_member &rm) { return rm.relation_id; }

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

template <typename T> void write_elements(output_writer &writer, control_block<T> &blk);

template <> inline void write_elements<node>(output_writer &writer, control_block<node> &blk) { 
  writer.nodes(blk.elements, blk.tags);
}
template <> inline void write_elements<way>(output_writer &writer, control_block<way> &blk) { 
  writer.ways(blk.elements, blk.inners, blk.tags);
}
template <> inline void write_elements<relation>(output_writer &writer, control_block<relation> &blk) { 
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

} // anonymous namespace

void extract_users(const std::string &dump_file, std::map<int64_t, std::string> &display_name_map) {
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

template void run_threads<node>(const std::string &, std::vector<boost::shared_ptr<output_writer> >);
template void run_threads<way>(const std::string &, std::vector<boost::shared_ptr<output_writer> >);
template void run_threads<relation>(const std::string &, std::vector<boost::shared_ptr<output_writer> >);
