#ifndef CHANGESET_MAP_HPP
#define CHANGESET_MAP_HPP

#include <vector>
#include <memory>
#include <boost/shared_ptr.hpp>

struct changeset_map {
  typedef int64_t * iterator;
  typedef const int64_t * const_iterator;
  typedef std::pair<int64_t, int64_t> value_type;

  void insert(const value_type &);

  const_iterator find(int64_t) const;
  const_iterator end() const;

private:
  std::vector<boost::shared_ptr<std::vector<int64_t> > > m_data;
};

#endif /* CHANGESET_MAP_HPP */
