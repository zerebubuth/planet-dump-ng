#ifndef OUTPUT_WRITER_HPP
#define OUTPUT_WRITER_HPP

#include <vector>
#include "types.hpp"

struct output_writer {
  virtual ~output_writer();
  virtual void nodes(const std::vector<node> &, const std::vector<old_tag> &) = 0;
  virtual void ways(const std::vector<way> &, const std::vector<way_node> &, const std::vector<old_tag> &) = 0;
  virtual void relations(const std::vector<relation> &, const std::vector<relation_member> &, const std::vector<old_tag> &) = 0;
};

#endif /* OUTPUT_WRITER */
