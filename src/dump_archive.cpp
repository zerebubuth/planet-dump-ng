#include "dump_archive.hpp"
#include "table_extractor.hpp"
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

namespace bt = boost::posix_time;
namespace fs = boost::filesystem;

namespace {

struct tag_table_name;
typedef boost::error_info<tag_table_name, std::string> errinfo_table_name;

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

} // anonymous namespace

base_thread::~base_thread() {}

template <typename R>
run_thread<R>::run_thread(std::string table_name_, std::string dump_file)
  : timestamp(), error(), 
    thr(&thread_extract_with_timestamp<R>,
        boost::ref(timestamp), boost::ref(error),
        table_name_, dump_file), table_name(table_name_) {
}

template <typename R>
run_thread<R>::~run_thread() {
  try {
    thr.join();
  } catch (...) {
  }
}

template <typename R>
bt::ptime run_thread<R>::join() {
  thr.join();
  if (error) {
    boost::throw_exception(boost::enable_error_info(std::runtime_error("Error during archive dump to LevelDB."))
                           << boost::errinfo_nested_exception(error)
                           << errinfo_table_name(table_name));
  }
  return timestamp;
}

template struct run_thread<user>;
template struct run_thread<changeset>;
template struct run_thread<current_tag>;
template struct run_thread<old_tag>;
template struct run_thread<node>;
template struct run_thread<way>;
template struct run_thread<way_node>;
template struct run_thread<relation>;
template struct run_thread<relation_member>;
template struct run_thread<changeset_comment>;
