#include "copy_elements.hpp"
#include "insert_kv.hpp"
#include "types.hpp"
#include "config.h"

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

#include <boost/filesystem.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/operations.hpp>
#include <fstream>

namespace bio = boost::iostreams;
namespace fs = boost::filesystem;

namespace {

template <typename T>
struct control_block {
  typedef typename T::tag_type tag_type;
  typedef typename T::inner_type inner_type;

  control_block(unsigned int num_threads)
  : pre_swap_barrier(num_threads),
    post_swap_barrier(num_threads),
    thread_status(num_threads, 0) {
  }

  boost::barrier pre_swap_barrier, post_swap_barrier;

  std::vector<int> thread_status;
  boost::mutex thread_finished_mutex;
  boost::condition_variable thread_finished_cond;

  std::vector<T> elements;
  std::vector<tag_type> tags;
  std::vector<inner_type> inners;
  std::vector<changeset_comment> comments;
};

template <typename T>
struct thread_writer {
  typedef typename T::tag_type tag_type;
  typedef typename T::inner_type inner_type;

  boost::shared_ptr<control_block<T> > blk;

  thread_writer(boost::shared_ptr<control_block<T> > b) : blk(b) {}

  void write(std::vector<T> &els, std::vector<inner_type> &inners, std::vector<tag_type> &tags) {
    blk->pre_swap_barrier.wait();
    std::swap(els, blk->elements);
    std::swap(inners, blk->inners);
    std::swap(tags, blk->tags);
    blk->post_swap_barrier.wait();
  }
};

template <typename T>
struct db_reader {
  explicit db_reader(const std::string &subdir) : m_end(false) {
    std::string file_name = (boost::format("%1$s/final_%2$08x.data") % subdir % 0).str();
    if (!fs::exists(file_name)) {
      BOOST_THROW_EXCEPTION(std::runtime_error((boost::format("File '%1%' does not exist.") % file_name).str()));
    }
    m_file.open(file_name.c_str());
    if (!m_file.is_open()) {
      BOOST_THROW_EXCEPTION(std::runtime_error((boost::format("Unable to open '%1%'.") % file_name).str()));
    }
    if (!m_file.good()) {
      BOOST_THROW_EXCEPTION(std::runtime_error((boost::format("File '%1%' is open, but not good.") % file_name).str()));
    }

    m_stream.push(bio::gzip_decompressor());
    m_stream.push(m_file);
  }

  ~db_reader() {
    bio::close(m_stream);
    m_file.close();
  }

  bool operator()(T &t) {
    if (m_end) { return false; }
    uint16_t ksz, vsz;
    
    if (bio::read(m_stream, (char *)&ksz, sizeof(uint16_t)) != sizeof(uint16_t)) { m_end = true; return false; }
    if (bio::read(m_stream, (char *)&vsz, sizeof(uint16_t)) != sizeof(uint16_t)) { m_end = true; return false; }

    std::string k, v;
    k.resize(ksz);
    if (bio::read(m_stream, &k[0], ksz) != ksz) { m_end = true; return false; }
    v.resize(vsz);
    if (bio::read(m_stream, &v[0], vsz) != vsz) { m_end = true; return false; }

    insert_kv(t, k, v);

    return true;
  }

private:
  bool m_end;
  std::ifstream m_file;
  bio::filtering_streambuf<bio::input> m_stream;
};

template <>
struct db_reader<int> {
  db_reader(const std::string &) {}
};

template <typename T> struct block_size_trait { static const size_t value = 1048576; };
template <> struct block_size_trait<relation> { static const size_t value =   65536; };

template <typename T> void zero_init(T &);
template <typename T> int64_t id_of(const T &);

template <> inline void zero_init<current_tag>(current_tag &t) { t.element_id = -1; }
template <> inline void zero_init<old_tag>(old_tag &t) { t.element_id = -1; }
template <> inline void zero_init<way_node>(way_node &wn) { wn.way_id = -1; }
template <> inline void zero_init<relation_member>(relation_member &rm) { rm.relation_id = -1; }
template <> inline void zero_init<changeset_comment>(changeset_comment &cc) { cc.changeset_id = -1; }
template <> inline void zero_init<int>(int &) { }

template <> inline int64_t id_of<current_tag>(const current_tag &t) { return t.element_id; }
template <> inline int64_t id_of<old_tag>(const old_tag &t) { return t.element_id; }
template <> inline int64_t id_of<way_node>(const way_node &wn) { return wn.way_id; }
template <> inline int64_t id_of<relation_member>(const relation_member &rm) { return rm.relation_id; }
template <> inline int64_t id_of<changeset_comment>(const changeset_comment &cc) { return cc.changeset_id; }

template <typename T>
inline int64_t version_of(const T &t) { return t.version; }

template <> inline int64_t version_of<changeset>(const changeset &) { return 0; }
template <> inline int64_t version_of<current_tag>(const current_tag &t) { return 0; }
template <> inline int64_t version_of<changeset_comment>(const changeset_comment &) { return 0; }

template <typename T>
inline void fetch_associated(T &t, int64_t id, int64_t version, db_reader<T> &reader, std::vector<T> &vec) {
  while ((id_of<T>(t) < id) || ((id_of<T>(t) == id) && (version_of<T>(t) <= version))) {
    if ((id_of<T>(t) == id) && (version_of<T>(t) == version)) {
      vec.push_back(t);
    }
    if (!reader(t)) {
      break;
    }
  }
}

template <>
inline void fetch_associated<int>(int &, int64_t, int64_t, db_reader<int> &, std::vector<int> &) {
}

template <typename T>
inline bool is_redacted(const T &t) { return t.redaction_id; }

template <> inline bool is_redacted<changeset>(const changeset &) { return false; }

template <typename T>
void extract_element(thread_writer<T> &writer) {
  typedef typename T::tag_type tag_type;
  typedef typename T::inner_type inner_type;

  const size_t block_size = block_size_trait<T>::value;

  db_reader<T> element_reader(T::table_name());
  db_reader<tag_type> tag_reader(T::tag_table_name());
  db_reader<inner_type> inner_reader(T::inner_table_name());

  std::vector<T> elements;
  std::vector<tag_type> tags;
  std::vector<inner_type> inners;

  elements.resize(block_size);
  size_t i = 0;

  tag_type current_tag;
  inner_type current_inner;

  zero_init<tag_type>(current_tag);
  zero_init<inner_type>(current_inner);

  while (element_reader(elements[i])) {
    // skip all redacted elements - they don't appear in the output
    // at all.
    if (is_redacted<T>(elements[i])) { continue; }

    // skip all negative ID elements - these shouldn't appear in the
    // database at all.
    if (elements[i].id < 0) { continue; }

    fetch_associated(current_inner, elements[i].id, version_of(elements[i]), inner_reader, inners);
    fetch_associated(current_tag, elements[i].id, version_of(elements[i]), tag_reader, tags);

    ++i;
    if (i == block_size) {
      writer.write(elements, inners, tags);
      inners.clear();
      tags.clear();
      i = 0;
      if (elements.size() != block_size) { elements.resize(block_size); }
    }
  }

  elements.resize(i);
  writer.write(elements, inners, tags);
}

template <typename T> void write_elements(output_writer &writer, control_block<T> &blk);

template <> inline void write_elements<changeset>(output_writer &writer, control_block<changeset> &blk) {
  writer.changesets(blk.elements, blk.tags, blk.inners);
}
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
  const size_t block_size = block_size_trait<T>::value;

  try {
    do {
      blk->pre_swap_barrier.wait();
      blk->post_swap_barrier.wait();
      
      write_elements<T>(*writer, *blk);
      
    } while (blk->elements.size() == block_size);

  } catch (...) {
    exc = boost::current_exception();
    std::cerr << "EXCEPTION: writer_thread(" << thread_index << "): " 
              << boost::diagnostic_information(exc) << std::endl;
  }

  try {
    boost::lock_guard<boost::mutex> lock(blk->thread_finished_mutex);
    blk->thread_status[thread_index] = 1;
    blk->thread_finished_cond.notify_one();

  } catch (...) {
    // this is a difficult case to handle - it's possible for locking
    // to fail, but unless we signal the condition variable then the
    // program would hang. instead, treat this as a fatal error.
    std::cerr << "Thread " << thread_index << " failed to lock mutex!\n";
    abort();
  }
}

void join_all_but(size_t i, std::vector<boost::shared_ptr<boost::thread> > &threads) {
  bool still_running = true;

  while (still_running) {
    still_running = false;

    for (size_t j = 0; j < threads.size(); ++j) {
      if ((j != i) && threads[j]->joinable()) {
        // if the thread isn't ready to join for a second, then it is probably blocked
        // on something - this is the exceptional path, so the likely case is that some
        // thread has thrown an exception and the rest are waiting for it at the
        // barrier.
        if (!threads[j]->timed_join(boost::posix_time::time_duration(0, 0, 1))) {
          still_running = true;
          threads[j]->interrupt();
        }
      }
    }
  }
}

} // anonymous namespace

void extract_users(std::map<int64_t, std::string> &display_name_map) {
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
                   boost::shared_ptr<control_block<T> > blk) {
  try {
    thread_writer<T> writer(blk);
    extract_element<T>(writer);

  } catch (...) {
    exc = boost::current_exception();
    std::cerr << "EXCEPTION: reader_thread(" << thread_index << "): " 
              << boost::diagnostic_information(exc) << std::endl;
  }

  try {
    boost::lock_guard<boost::mutex> lock(blk->thread_finished_mutex);
    blk->thread_status[thread_index] = 1;
    blk->thread_finished_cond.notify_one();

  } catch (...) {
    // this is a difficult case to handle - it's possible for locking
    // to fail, but unless we signal the condition variable then the
    // program would hang. instead, treat this as a fatal error.
    std::cerr << "Thread " << thread_index << " failed to lock mutex!\n";
    abort();
  }
}

template <typename T>
void run_threads(std::vector<boost::shared_ptr<output_writer> > writers) {
  std::vector<boost::shared_ptr<boost::thread> > threads;
  std::vector<boost::exception_ptr> exceptions;
  const int num_threads = writers.size() + 1;
  int i = 0, num_running_threads = num_threads;

  exceptions.resize(num_threads);
  boost::shared_ptr<control_block<T> > blk = boost::make_shared<control_block<T> >(writers.size() + 1);

  threads.push_back(boost::make_shared<boost::thread>(boost::bind(&reader_thread<T>, i, exceptions[i], blk)));

  BOOST_FOREACH(boost::shared_ptr<output_writer> writer, writers) {
    ++i;
    threads.push_back(boost::make_shared<boost::thread>(boost::bind(&writer_thread<T>, i, exceptions[i], writer, blk)));
  }

  {
    boost::unique_lock<boost::mutex> lock(blk->thread_finished_mutex);
    while (num_running_threads > 0) {
      blk->thread_finished_cond.wait(lock);

      for (int idx = 0; idx < num_threads; ++idx) {
        if (blk->thread_status[idx] != 0) {

          blk->thread_status[idx] = 0;

          boost::shared_ptr<boost::thread> thread = threads[idx];
          thread->join();
          --num_running_threads;

          if (exceptions[idx]) {
            lock.unlock();
            // interrupt all other threads and join them
            join_all_but(idx, threads);
            lock.lock();
            // re-throw the exception
            boost::rethrow_exception(exceptions[idx]);
          }
        }
      }
    }
  }
}

template void run_threads<node>(std::vector<boost::shared_ptr<output_writer> >);
template void run_threads<way>(std::vector<boost::shared_ptr<output_writer> >);
template void run_threads<relation>(std::vector<boost::shared_ptr<output_writer> >);
template void run_threads<changeset>(std::vector<boost::shared_ptr<output_writer> >);
