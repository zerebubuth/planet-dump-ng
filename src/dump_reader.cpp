#include "dump_reader.hpp"

#include <cstdio>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/format.hpp>
#include <boost/noncopyable.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <leveldb/db.h>
#include <leveldb/options.h>
#include <leveldb/write_batch.h>

#define BATCH_SIZE (10240)

namespace {

struct process 
  : public boost::noncopyable {
  explicit process(const std::string &cmd) 
    : m_fh(popen(cmd.c_str(), "r")) {
    if (m_fh == NULL) {
      throw std::runtime_error((boost::format("Unable to popen `%1%'.") % cmd).str());
    }
  }

  ~process() {
    if (pclose(m_fh) == -1) {
      std::cerr << "ERROR while closing popen." << std::endl;
      abort();
    }
  }

  size_t read(char *buf, size_t len) {
    size_t n = fread(buf, 1, len, m_fh);
    if (ferror(m_fh) != 0) {
      throw std::runtime_error("Error reading from popen stream.");
    }
    return n;
  }
    
private:
  FILE *m_fh;
};

template <typename T>
struct to_line_filter 
  : public boost::noncopyable {
  to_line_filter(T &source, size_t buffer_size) 
    : m_source(source), 
      m_buffer(buffer_size, '\0'),
      m_buffer_pos(m_buffer.begin()),
      m_buffer_end(m_buffer_pos) {
  }

  ~to_line_filter() {
  }

  size_t read(std::string &line) {
    line.clear();
    std::string::iterator begin_pos = m_buffer_pos;
    char c = '\0';

    do {
      if (m_buffer_pos == m_buffer_end) {
        line.append(begin_pos, m_buffer_pos);
        if (refill() == 0) {
          return 0;
        }
        begin_pos = m_buffer_pos;
      }

      c = *m_buffer_pos;
      if (c != '\n') {
        ++m_buffer_pos;
      }
    } while (c != '\n');

    line.append(begin_pos, m_buffer_pos);
    ++m_buffer_pos;

    return 1;
  }

private:
  size_t refill() {
    size_t bytes = 0;
    while (bytes < m_buffer.size()) {
      size_t len = m_source.read(&m_buffer[bytes], m_buffer.size() - bytes);
      if (len == 0) {
        break;
      }
      bytes += len;
    }
    m_buffer_pos = m_buffer.begin();
    m_buffer_end = m_buffer.begin() + bytes;
    return bytes;
  }

  T &m_source;
  std::string m_buffer;
  std::string::iterator m_buffer_pos, m_buffer_end;
};

template <typename T>
struct filter_copy_contents 
  : public boost::noncopyable {
  explicit filter_copy_contents(T &source) 
  : m_source(source),
    m_in_copy(false),
    m_start_prefix("COPY "),
    m_end_line("\\.") {
  }

  ~filter_copy_contents() {
  }

  size_t read(std::string &line) {
    size_t got_data = 0;
    do {
      got_data = m_source.read(line);

      if (got_data == 0) {
        break;
      }

      if (!m_in_copy && (line.compare(0, m_start_prefix.size(), m_start_prefix) == 0)) {
        m_in_copy = true;
        got_data = m_source.read(line);
      }

      if (m_in_copy && (line.compare(m_end_line) == 0)) {
        m_in_copy = false;
      }
    } while (!m_in_copy);

    return got_data;
  }

private:
  T &m_source;
  bool m_in_copy;
  const std::string m_start_prefix, m_end_line;
};

} // anonymous namespace

struct dump_reader::pimpl {
  pimpl(const std::string &cmd, const std::string &table_name)
    : m_proc(cmd),
      m_line_filter(m_proc, 1024 * 1024),
      m_cont_filter(m_line_filter),
      m_db(NULL),
      m_batch(),
      m_batch_size(0),
      m_write_options() {

    leveldb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;
    leveldb::Status status = leveldb::DB::Open(options, table_name, &m_db);
    if (!status.ok()) {
      throw std::runtime_error((boost::format("Can't open database: %1%") % status.ToString()).str());
    }
  }

  ~pimpl() {
    m_db->CompactRange(NULL, NULL);
    delete m_db;
  }

  void put(const std::string &k, const std::string &v) {
    m_batch.Put(k, v);
    ++m_batch_size;

    if (m_batch_size >= BATCH_SIZE) {
      m_db->Write(m_write_options, &m_batch);
      m_batch.Clear();
      m_batch_size = 0;
    }
  }
  
  process m_proc;
  to_line_filter<process> m_line_filter;
  filter_copy_contents<to_line_filter<process> > m_cont_filter;
  
  leveldb::DB *m_db;
  leveldb::WriteBatch m_batch;
  size_t m_batch_size;
  leveldb::WriteOptions m_write_options;
};

dump_reader::dump_reader(const std::string &table_name,
                         const std::string &dump_file) 
  : m_impl() {
  std::ostringstream cmd;
  cmd << "pg_restore -a -t " << table_name << " " << dump_file;
  m_impl.reset(new pimpl(cmd.str(), table_name));
}

dump_reader::~dump_reader() {
}  

size_t dump_reader::read(std::string &line) {
  return m_impl->m_cont_filter.read(line);
}

void dump_reader::put(const std::string &k, const std::string &v) {
  m_impl->put(k, v);
}
