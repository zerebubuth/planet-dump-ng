#ifndef OUTPUT_WRITER_HPP
#define OUTPUT_WRITER_HPP

#include <boost/noncopyable.hpp>
#include <vector>
#include "types.hpp"

/**
 * generic output sink for OSM element types.
 *
 * this interface is designed to be able to reasonably efficiently dump out
 * chunks of OSM element types, while still being generic enough to handle
 * output to XML, PBF and any other file types which we would want to write.
 */
struct output_writer : private boost::noncopyable {
  typedef std::map<int64_t, std::string> user_map_t;

  virtual ~output_writer();

  // dump a chunk of elements. included are the associated tags and other
  // inner types for that element. the chunk will be already ordered and
  // the inner types ordered by the (id, version) of their element.
  virtual void nodes(const std::vector<node> &, const std::vector<old_tag> &) = 0;
  virtual void ways(const std::vector<way> &, const std::vector<way_node> &, const std::vector<old_tag> &) = 0;
  virtual void relations(const std::vector<relation> &, const std::vector<relation_member> &, const std::vector<old_tag> &) = 0;

  // called once, at the end of the writing process. at this point the
  // output writer should write any remaining data, flush the output
  // file and close it. anything which could throw should be in here,
  // not in the destructor.
  virtual void finish() = 0;
};

#endif /* OUTPUT_WRITER */
