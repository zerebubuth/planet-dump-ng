#ifndef WRITER_COMMON_HPP
#define WRITER_COMMON_HPP

#include <string>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/program_options.hpp>

#define OSM_LICENSE_TEXT     "http://opendatacommons.org/licenses/odbl/1-0/"
#define OSM_COPYRIGHT_TEXT   "OpenStreetMap and contributors"
#define OSM_VERSION_TEXT     "0.6"
#define OSM_ATTRIBUTION_TEXT "http://www.openstreetmap.org/copyright"
#define OSM_API_ORIGIN       "http://www.openstreetmap.org/api/0.6"

/**
 * helpers shared by the XML writers (xml_writer and gpx_writer).
 */

// replace characters which are valid UTF-8, but invalid XML.
std::string replace_xml_bad_chars(const std::string &s);

// shell command which compresses stdin into file_name.
std::string popen_command(const std::string &file_name, const boost::program_options::variables_map &options);

// format a time as YYYY-mm-ddTHH:MM:SSZ, or an empty string if it is special.
std::string fmt_iso_time(const boost::posix_time::ptime &t);

#endif /* WRITER_COMMON_HPP */
