#include "dump_reader.hpp"
#include "config.h"

#include <cstdio>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/format.hpp>
#include <boost/noncopyable.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#ifdef HAVE_LEVELDB
#include <leveldb/db.h>
#include <leveldb/options.h>
#include <leveldb/write_batch.h>
#endif /* HAVE_LEVELDB */

#include <boost/spirit/include/qi.hpp>
#include <boost/foreach.hpp>
#include <boost/exception/all.hpp>
#include <boost/throw_exception.hpp>
#include <boost/exception/error_info.hpp>
#include <boost/weak_ptr.hpp>

#define BATCH_SIZE (10240)

namespace {

namespace qi = boost::spirit::qi;

struct tag_copy_header;
struct tag_leveldb_status;

typedef boost::error_info<tag_copy_header, std::string>    copy_header;
typedef boost::error_info<tag_leveldb_status, std::string> leveldb_status;

struct popen_error : public boost::exception, std::exception {};
struct fread_error : public boost::exception, std::exception {};
struct early_termination_error : public boost::exception, std::exception {};
struct copy_header_parse_error : public boost::exception, std::exception {};
struct leveldb_error : public boost::exception, std::exception {};

typedef boost::shared_ptr<FILE> pipe_ptr;

static void pipe_closer(FILE *fh) {
  if (fh != NULL) {
    if (pclose(fh) == -1) {
      std::cerr << "ERROR while closing popen." << std::endl;
      abort();
    }
  }
}

struct process 
  : public boost::noncopyable {
  explicit process(const std::string &cmd) 
    : m_fh(popen(cmd.c_str(), "r"), &pipe_closer) {
    if (!m_fh) {
      BOOST_THROW_EXCEPTION(popen_error() << boost::errinfo_file_name(cmd));
    }
  }

  ~process() {
  }

  size_t read(char *buf, size_t len) {
    size_t n = fread(buf, 1, len, m_fh.get());
    if (ferror(m_fh.get()) != 0) {
      boost::weak_ptr<FILE> fh(m_fh);
      BOOST_THROW_EXCEPTION(fread_error() << boost::errinfo_file_handle(fh));
    }
    return n;
  }
    
private:
  pipe_ptr m_fh;
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

// COPY current_nodes (id, latitude, longitude, changeset_id, visible, "timestamp", tile, version) FROM stdin;
template <typename Iterator>
struct copy_line
  : qi::grammar<Iterator, std::vector<std::string>(), qi::space_type> {

  copy_line(const std::string &table_name)
    : copy_line::base_type(root) {
    using qi::char_;
    using qi::alpha;
    using qi::alnum;
    using qi::lexeme;
    using qi::lit;

    root = lit("COPY") >> lit(table_name) >> lit("(") >> (ident % lit(',')) >> lit(") FROM stdin;");
    ident = (alpha >> *(alnum | char_('_'))) | (lit("\"") >> *(char_ - '"' - '\\') >> lit("\""));
  }

  qi::rule<Iterator, std::vector<std::string>(), qi::space_type> root;
  qi::rule<Iterator, std::string(), qi::space_type> ident;
};

template <typename T>
struct filter_copy_contents 
  : public boost::noncopyable {
  explicit filter_copy_contents(T &source, const std::string &table_name) 
  : m_source(source),
    m_in_copy(false),
    m_start_prefix("COPY "),
    m_end_line("\\."),
    m_grammar(table_name),
    m_table_name(table_name) {
  }

  ~filter_copy_contents() {
  }

  std::vector<std::string> init() {
    std::vector<std::string> column_names;
    std::string line;
    size_t got_data = 0;

    do {
      got_data = m_source.read(line);
      
      if (got_data == 0) {
        BOOST_THROW_EXCEPTION(early_termination_error());
      }

      if (line.compare(0, m_start_prefix.size(), m_start_prefix) == 0) {
        std::string::iterator begin = line.begin();
        std::string::iterator end = line.end();
        bool result = qi::phrase_parse(begin, end, m_grammar, qi::space, column_names);
        if (!result) {
          BOOST_THROW_EXCEPTION(copy_header_parse_error() << copy_header(line));
        }
        m_in_copy = true;
        break;
      }
    } while (true);

    if (!m_in_copy) {
      BOOST_THROW_EXCEPTION(early_termination_error());
    }
    if (column_names.empty()) {
      BOOST_THROW_EXCEPTION(early_termination_error());
    }

    return column_names;
  }

  size_t read(std::string &line) {
    size_t got_data = 0;
    do {
      got_data = m_source.read(line);

      if (got_data == 0) {
        break;
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
  copy_line<std::string::iterator> m_grammar;
  std::string m_table_name;
};

#ifdef HAVE_LEVELDB  
struct db_writer {
  explicit db_writer(const std::string &table_name)
    : m_db(NULL),
      m_batch(),
      m_batch_size(0),
      m_write_options() {

    leveldb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;

    // bigger write buffer, as this is a write-heavy process...
    options.write_buffer_size = 128 * 1024 * 1024;

    leveldb::Status status = leveldb::DB::Open(options, table_name, &m_db);
    if (!status.ok()) {
      BOOST_THROW_EXCEPTION(leveldb_error() << leveldb_status(status.ToString()));
    }
  }

  ~db_writer() {
    if (m_batch_size > 0) {
      m_db->Write(m_write_options, &m_batch);
      m_batch.Clear();
      m_batch_size = 0;
    }
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
  
  leveldb::DB *m_db;
  leveldb::WriteBatch m_batch;
  size_t m_batch_size;
  leveldb::WriteOptions m_write_options;
};

#else /* HAVE_LEVELDB */

struct db_writer {
  explicit db_writer(const std::string &table_name) {
    // TODO MERGESORT
  }

  ~db_writer() {
    // TODO MERGESORT
  }

  void put(const std::string &k, const std::string &v) {
    // TODO MERGESORT
  }
};
#endif /* HAVE_LEVELDB */

} // anonymous namespace

struct dump_reader::pimpl {
  pimpl(const std::string &cmd, const std::string &table_name)
    : m_proc(cmd),
      m_line_filter(m_proc, 1024 * 1024),
      m_cont_filter(m_line_filter, table_name),
      m_writer(table_name) {

    // get the headers for the COPY data
    m_column_names = m_cont_filter.init();
  }

  ~pimpl() {
  }

  process m_proc;
  to_line_filter<process> m_line_filter;
  filter_copy_contents<to_line_filter<process> > m_cont_filter;

  db_writer m_writer;

  std::vector<std::string> m_column_names;
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

const std::vector<std::string> &dump_reader::column_names() const {
  return m_impl->m_column_names;
}

size_t dump_reader::read(std::string &line) {
  return m_impl->m_cont_filter.read(line);
}

void dump_reader::put(const std::string &k, const std::string &v) {
  m_impl->m_writer.put(k, v);
}
