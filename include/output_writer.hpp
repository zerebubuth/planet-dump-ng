#ifndef OUTPUT_WRITER_HPP
#define OUTPUT_WRITER_HPP

#include <vector>
#include "types.hpp"

struct output_writer {
  virtual ~output_writer();
  virtual void nodes(const std::vector<current_node> &, const std::vector<current_tag> &) = 0;
  virtual void ways(const std::vector<current_way> &, const std::vector<current_way_node> &, const std::vector<current_tag> &) = 0;
  virtual void relations(const std::vector<current_relation> &, const std::vector<current_relation_member> &, const std::vector<current_tag> &) = 0;
};

#endif /* OUTPUT_WRITER */
