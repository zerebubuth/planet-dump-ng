#ifndef DUMP_READER_HPP
#define DUMP_READER_HPP

#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <string>
#include <vector>

struct dump_reader 
  : public boost::noncopyable {
  dump_reader(const std::string &table_name,
              const std::string &dump_file,
              unsigned int max_concurrency);

  ~dump_reader();

  const std::vector<std::string> &column_names() const;
  size_t read(std::string &);
  void put(const std::string &, const std::string &);
  void finish();

private:
  struct pimpl;
  boost::scoped_ptr<pimpl> m_impl;
};

#endif /* DUMP_READER_HPP */
