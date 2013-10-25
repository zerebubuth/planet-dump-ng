#ifndef COPY_ELEMENTS_HPP
#define COPY_ELEMENTS_HPP

#include "output_writer.hpp"
#include <boost/shared_ptr.hpp>
#include <vector>
#include <string>

void extract_users(const std::string &dump_file, std::map<int64_t, std::string> &display_name_map);

template <typename T>
void run_threads(const std::string &dump_file, 
                 std::vector<boost::shared_ptr<output_writer> > writers);

#endif /* COPY_ELEMENTS_HPP */
