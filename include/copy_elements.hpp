#ifndef COPY_ELEMENTS_HPP
#define COPY_ELEMENTS_HPP

#include "output_writer.hpp"
#include <boost/shared_ptr.hpp>
#include <vector>
#include <set>
#include <string>

/**
 * Read the disk database for users, and extract all the public data
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

class gpx_writer;

/**
 * Read the disk databases for gpx_files, gpx_tracks and gpx_file_tags, and
 * write the visible traces with one of the given visibilities to the GPX
 * writer.
 */
void write_gpx(gpx_writer &writer, const std::set<std::string> &visibilities);

#endif /* COPY_ELEMENTS_HPP */
