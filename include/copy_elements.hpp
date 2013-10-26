#ifndef COPY_ELEMENTS_HPP
#define COPY_ELEMENTS_HPP

#include "output_writer.hpp"
#include <boost/shared_ptr.hpp>
#include <vector>
#include <string>

/**
 * Read the LevelDB database for users, and extract all the public data
 * ones into a map of user ID to display name.
 */
void extract_users(std::map<int64_t, std::string> &display_name_map);

/**
 * Copy the elements (and associated tags, way nodes, etc...) for
 * some type T, and write them in parallel threads to all of the
 * writers.
 */
template <typename T>
void run_threads(std::vector<boost::shared_ptr<output_writer> > writers);

#endif /* COPY_ELEMENTS_HPP */
