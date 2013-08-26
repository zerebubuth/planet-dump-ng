#ifndef DUMP_READER_HPP
#define DUMP_READER_HPP

#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <string>

struct dump_reader 
  : public boost::noncopyable {
  dump_reader(const std::string &,
              const std::string &);

  ~dump_reader();

  size_t read(std::string &);
  void put(const std::string &, const std::string &);

private:
  struct pimpl;
  boost::scoped_ptr<pimpl> m_impl;
};

#endif /* DUMP_READER_HPP */
