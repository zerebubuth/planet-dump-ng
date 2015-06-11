#ifndef DUMP_ARCHIVE_HPP
#define DUMP_ARCHIVE_HPP

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/exception/all.hpp>
#include <boost/thread.hpp>
#include <string>
#include <map>
#include "stdint.h"

struct base_thread {
  virtual ~base_thread();
  virtual boost::posix_time::ptime join() = 0;
};

template <typename R>
struct run_thread : public base_thread {
  boost::posix_time::ptime timestamp;
  boost::exception_ptr error;
  boost::thread thr;
  std::string table_name;

  run_thread(std::string table_name_, std::string dump_file, bool resume);
  ~run_thread();
  boost::posix_time::ptime join();
};

#endif /* DUMP_ARCHIVE_HPP */
