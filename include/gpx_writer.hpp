#ifndef GPX_WRITER_HPP
#define GPX_WRITER_HPP

#include "types.hpp"
#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/program_options.hpp>
#include <string>
#include <vector>

/**
 * writes GPS traces to a GPX 1.0 file: one <trk> per trace and one <trkseg>
 * per trackid. every trace has its id as osm:id. identifiable traces also
 * have name, description and tags.
 */
class gpx_writer : private boost::noncopyable {
public:
  gpx_writer(const std::string &, const boost::program_options::variables_map &,
             const boost::posix_time::ptime &max_time);
  ~gpx_writer();

  // write one trace. the tracks must be ordered by (trackid, segment).
  void trace(const gpx_file &, const std::vector<gpx_track> &, const std::vector<gpx_file_tag> &);

  // close the document and the output stream.
  void finish();

  struct pimpl;

private:
  boost::scoped_ptr<pimpl> m_impl;
  std::string m_generator_name;
  std::string m_author_name;
  std::string m_copyleft_name;
};

#endif /* GPX_WRITER_HPP */
