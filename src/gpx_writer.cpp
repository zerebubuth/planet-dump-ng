#include "gpx_writer.hpp"
#include "config.h"
#include "writer_common.hpp"

#include <libxml/encoding.h>
#include <libxml/xmlwriter.h>

#include <cmath>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/exception/all.hpp>
#include <boost/date_time/posix_time/conversion.hpp>

namespace pt = boost::posix_time;

namespace {

const char *OSM_NAMESPACE = "https://www.openstreetmap.org/gpx/0.1";

struct point {
  double lon, lat, ele, time;
};

/**
 * the M value of a point holds its time as seconds since the epoch. the
 * fractional seconds are dropped, as in the trackpoints API.
 */
pt::ptime epoch_to_time(double seconds) {
  return pt::from_time_t(time_t(std::floor(seconds)));
}

int hex_value(char c) {
  if ((c >= '0') && (c <= '9')) { return c - '0'; }
  if ((c >= 'a') && (c <= 'f')) { return 10 + c - 'a'; }
  if ((c >= 'A') && (c <= 'F')) { return 10 + c - 'A'; }
  BOOST_THROW_EXCEPTION(std::runtime_error("Invalid hex digit in geometry."));
}

/**
 * reads the hex EWKB that PostGIS writes in COPY output. gpx_tracks only has
 * LineString and Point with Z and M, but any of XY, XYZ, XYM and XYZM is
 * accepted. Z is the altitude and M is the time.
 */
struct ewkb_reader {
  explicit ewkb_reader(const std::string &hex) : m_pos(0), m_little_endian(true) {
    if (hex.size() % 2 != 0) {
      BOOST_THROW_EXCEPTION(std::runtime_error("Odd length of hex geometry."));
    }
    m_bytes.resize(hex.size() / 2);
    for (size_t i = 0; i < m_bytes.size(); ++i) {
      m_bytes[i] = (unsigned char)((hex_value(hex[2 * i]) << 4) | hex_value(hex[2 * i + 1]));
    }
  }

  void read(std::vector<point> &points) {
    m_little_endian = (read_byte() == 1);
    uint32_t type = read_uint32();
    const bool has_z = (type & 0x80000000) != 0;
    const bool has_m = (type & 0x40000000) != 0;
    const bool has_srid = (type & 0x20000000) != 0;
    type &= 0x0fffffff;

    if (has_srid) {
      read_uint32();
    }

    uint32_t count = 0;
    if (type == 1) {
      count = 1;
    } else if (type == 2) {
      count = read_uint32();
    } else {
      BOOST_THROW_EXCEPTION(std::runtime_error((boost::format("Unexpected geometry type %1%.") % type).str()));
    }

    for (uint32_t i = 0; i < count; ++i) {
      point p;
      p.lon = read_double();
      p.lat = read_double();
      p.ele = has_z ? read_double() : 0.0;
      p.time = has_m ? read_double() : -INFINITY;
      // a POINT EMPTY is written with NaN coordinates
      if (std::isnan(p.lon) || std::isnan(p.lat)) {
        continue;
      }
      points.push_back(p);
    }
  }

private:
  unsigned char read_byte() {
    if (m_pos >= m_bytes.size()) {
      BOOST_THROW_EXCEPTION(std::runtime_error("Geometry is too short."));
    }
    return m_bytes[m_pos++];
  }

  uint64_t read_uint(int size) {
    uint64_t v = 0;
    for (int i = 0; i < size; ++i) {
      uint64_t b = read_byte();
      if (m_little_endian) {
        v |= b << (8 * i);
      } else {
        v = (v << 8) | b;
      }
    }
    return v;
  }

  uint32_t read_uint32() { return uint32_t(read_uint(4)); }

  double read_double() {
    uint64_t v = read_uint(8);
    double d;
    std::memcpy(&d, &v, sizeof(d));
    return d;
  }

  std::vector<unsigned char> m_bytes;
  size_t m_pos;
  bool m_little_endian;
};

} // anonymous namespace

struct gpx_writer::pimpl {
  pimpl(const std::string &file_name, const boost::program_options::variables_map &options);
  ~pimpl();

  void begin(const char *name);
  void attribute(const char *name, double d);
  void attribute(const char *name, const char *s);
  void attribute(const char *name, const std::string &s);
  void end();

  void element(const char *name, int64_t i);
  void element(const char *name, double d);
  void element(const char *name, const pt::ptime &t);
  void element(const char *name, const std::string &s);

  // flush & close output stream
  void finish();

  std::string m_command;
  FILE *m_out;
  xmlTextWriterPtr m_writer;
};

static int wrap_write(void *context, const char *buffer, int len) {
  gpx_writer::pimpl *impl = static_cast<gpx_writer::pimpl *>(context);

  if (impl == NULL) {
    BOOST_THROW_EXCEPTION(std::runtime_error("State object NULL in wrap_write."));
  }
  if (impl->m_out == NULL) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Output pipe NULL in wrap_write."));
  }

  if (len < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Negative length in wrap_write."));
  }
  const size_t slen = len;

  const size_t status = fwrite(buffer, 1, slen, impl->m_out);
  if (status < slen) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Failed to write to output stream."));
  }
  return len;
}

static int wrap_close(void *context) {
  gpx_writer::pimpl *impl = static_cast<gpx_writer::pimpl *>(context);

  if (impl == NULL) {
    BOOST_THROW_EXCEPTION(std::runtime_error("State object NULL in wrap_close."));
  }
  if (impl->m_out == NULL) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Output pipe NULL in wrap_close."));
  }

  int status = pclose(impl->m_out);
  if (status == -1) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Output pipe could not be closed in wrap_close."));
  }
  impl->m_out = NULL;

  return 0;
}

gpx_writer::pimpl::pimpl(const std::string &file_name, const boost::program_options::variables_map &options)
  : m_command(popen_command(file_name, options)), m_out(popen(m_command.c_str(), "w")),
    m_writer(NULL) {

  if (m_out == NULL) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to popen compression command for output."));
  }

  xmlOutputBufferPtr output_buffer =
    xmlOutputBufferCreateIO(wrap_write, wrap_close, this, NULL);

  m_writer = xmlNewTextWriter(output_buffer);
  if (m_writer == NULL) {
    free(output_buffer);

    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to create xmlTextWriter."));
  }

  xmlTextWriterSetIndent(m_writer, 1);
  if (xmlTextWriterStartDocument(m_writer, NULL, "UTF-8", NULL) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to start document."));
  }
}

gpx_writer::pimpl::~pimpl() {
}

void gpx_writer::pimpl::finish() {
  try {
    xmlTextWriterEndDocument(m_writer);
  } catch (...) {
  }
  xmlFreeTextWriter(m_writer);

  if (m_out != NULL) {
    // note that this *should* have already happened in xmlFreeTextWriter
    // but this is just to be on the safe side and not leave any processes
    // lying around.
    pclose(m_out);
  }
}

void gpx_writer::pimpl::begin(const char *name) {
  if (xmlTextWriterStartElement(m_writer, BAD_CAST name) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to begin element XML."));
  }
}

void gpx_writer::pimpl::attribute(const char *name, double d) {
  if (xmlTextWriterWriteFormatAttribute(m_writer,
                                        BAD_CAST name,
                                        "%.7f", d) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to write double attribute."));
  }
}

void gpx_writer::pimpl::attribute(const char *name, const char *s) {
  std::string fixed = replace_xml_bad_chars(s);
  if (xmlTextWriterWriteAttribute(m_writer,
                                  BAD_CAST name,
                                  BAD_CAST fixed.c_str()) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to write string attribute."));
  }
}

void gpx_writer::pimpl::attribute(const char *name, const std::string &s) {
  attribute(name, s.c_str());
}

void gpx_writer::pimpl::end() {
  if (xmlTextWriterEndElement(m_writer) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to end element XML."));
  }
}

void gpx_writer::pimpl::element(const char *name, int64_t i) {
  if (xmlTextWriterWriteFormatElement(m_writer,
                                      BAD_CAST name,
                                      "%ld", i) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to write int64 element."));
  }
}

// only used for the altitude, which is written with two decimals.
void gpx_writer::pimpl::element(const char *name, double d) {
  if (xmlTextWriterWriteFormatElement(m_writer,
                                      BAD_CAST name,
                                      "%.2f", d) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to write double element."));
  }
}

void gpx_writer::pimpl::element(const char *name, const pt::ptime &t) {
  std::string ts = fmt_iso_time(t);
  if (xmlTextWriterWriteElement(m_writer,
                                BAD_CAST name,
                                BAD_CAST ts.c_str()) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to write timestamp element."));
  }
}

void gpx_writer::pimpl::element(const char *name, const std::string &s) {
  std::string fixed = replace_xml_bad_chars(s);
  if (xmlTextWriterWriteElement(m_writer,
                                BAD_CAST name,
                                BAD_CAST fixed.c_str()) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to write string element."));
  }
}

gpx_writer::gpx_writer(const std::string &file_name, const boost::program_options::variables_map &options,
                       const pt::ptime &max_time)
  : m_impl(new pimpl(file_name, options))
  , m_generator_name(options["generator"].as<std::string>())
  , m_author_name(options["meta-author"].as<std::string>())
  , m_copyleft_name(options["meta-copyleft"].as<std::string>()) {

  m_impl->begin("gpx");
  m_impl->attribute("version",   "1.0");
  m_impl->attribute("creator",   m_generator_name);
  m_impl->attribute("xmlns",     "http://www.topografix.com/GPX/1/0");
  m_impl->attribute("xmlns:osm", OSM_NAMESPACE);

  m_impl->element("author", m_author_name);
  m_impl->element("url", m_copyleft_name);
  if (!max_time.is_special()) {
    m_impl->element("time", max_time);
  }
}

gpx_writer::~gpx_writer() {
}

void gpx_writer::trace(const gpx_file &file, const std::vector<gpx_track> &tracks,
                       const std::vector<gpx_file_tag> &tags) {
  // decode first, so a broken geometry skips the whole trace and does not
  // leave a half written <trk>.
  std::vector<std::vector<point> > trksegs;
  try {
    int32_t trackid = -1;
    BOOST_FOREACH(const gpx_track &track, tracks) {
      // segments split one track by size or distance, and do not share
      // points, so all the segments of a trackid go in one <trkseg>.
      if (trksegs.empty() || (track.trackid != trackid)) {
        trksegs.push_back(std::vector<point>());
        trackid = track.trackid;
      }
      ewkb_reader(track.geom).read(trksegs.back());
    }
  } catch (const std::exception &e) {
    std::cerr << "WARNING: skipping trace " << file.id << ": " << e.what() << std::endl;
    return;
  }

  m_impl->begin("trk");

  // only identifiable traces have a name, a description and tags.
  // trackable traces only have the id and the points.
  const bool identifiable = (file.visibility == "identifiable");
  if (identifiable) {
    m_impl->element("name", file.name);
    m_impl->element("desc", file.description);
  }
  m_impl->element("osm:id", file.id);
  if (identifiable) {
    BOOST_FOREACH(const gpx_file_tag &tag, tags) {
      m_impl->element("osm:tag", tag.tag);
    }
  }

  BOOST_FOREACH(const std::vector<point> &trkseg, trksegs) {
    if (trkseg.empty()) {
      continue;
    }
    m_impl->begin("trkseg");
    BOOST_FOREACH(const point &p, trkseg) {
      m_impl->begin("trkpt");
      m_impl->attribute("lat", p.lat);
      m_impl->attribute("lon", p.lon);
      // gpx_tracks saves a missing altitude as 0, so 0 is left out.
      if (p.ele != 0.0) {
        m_impl->element("ele", p.ele);
      }
      // -Infinity is a point without a timestamp.
      if (std::isfinite(p.time)) {
        m_impl->element("time", epoch_to_time(p.time));
      }
      m_impl->end();
    }
    m_impl->end();
  }

  m_impl->end();
}

void gpx_writer::finish() {
  m_impl->finish();
}
