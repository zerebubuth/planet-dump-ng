#include "gpx_writer.hpp"
#include "config.h"
#include "writer_common.hpp"

#include <libxml/encoding.h>
#include <libxml/xmlwriter.h>

#include <cmath>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <iostream>
#include <stdexcept>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/exception/all.hpp>

namespace pt = boost::posix_time;

namespace {

const char *OSM_NAMESPACE = "https://www.openstreetmap.org/gpx/0.1";

struct shell_escape_char {
  template <typename ResultT>
  std::string operator()(const ResultT &result) const {
    std::string s;
    for (typename ResultT::const_iterator itr = result.begin();
         itr != result.end(); ++itr) {
      s += '\\';
      s += *itr;
    }
    return s;
  }
};

std::string popen_command(const std::string &file_name, const boost::program_options::variables_map &options) {
  std::string compress_command = options["compress-command"].as<std::string>();

  std::string escaped_file_name(file_name);
  boost::find_format_all(escaped_file_name, boost::token_finder(boost::is_any_of("\\\"")), shell_escape_char());

  std::ostringstream command;
  command << compress_command << " > \"" << escaped_file_name << "\"";
  return command.str();
}

// same as in xml_writer: remove ASCII control characters, which are valid
// UTF-8 but not valid XML.
std::string replace_xml_bad_chars(const std::string &s) {
  std::string output(s);
  for (size_t i = 0; i < output.size(); ++i) {
    char c = output[i];
    if ((c >= 0x00) && (c < 0x20) && (c != 0x09) && (c != 0x0a) && (c != 0x0d)) {
      output[i] = '?';
    }
  }
  return output;
}

std::string fmt_iso_time(const pt::ptime &t) {
  std::string s = pt::to_iso_extended_string(t);
  // drop fractional seconds, if any
  size_t dot = s.find('.');
  if (dot != std::string::npos) {
    s.resize(dot);
  }
  return s + "Z";
}

// M holds the time as seconds since the epoch.
std::string fmt_epoch(double seconds) {
  time_t t = time_t(std::floor(seconds));
  struct tm tm;
  if (gmtime_r(&t, &tm) == NULL) {
    return std::string();
  }
  char buf[32];
  size_t len = strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
  return std::string(buf, len);
}

struct point {
  double lon, lat, ele, time;
};

int hex_value(char c) {
  if (c >= '0' && c <= '9') { return c - '0'; }
  if (c >= 'a' && c <= 'f') { return 10 + c - 'a'; }
  if (c >= 'A' && c <= 'F') { return 10 + c - 'A'; }
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

  void begin(const char *name);
  void end();
  void attribute(const char *name, const char *value);
  void element(const char *name, const std::string &value);
  void finish();

  std::string m_command;
  FILE *m_out;
  xmlTextWriterPtr m_writer;
};

static int gpx_wrap_write(void *context, const char *buffer, int len) {
  gpx_writer::pimpl *impl = static_cast<gpx_writer::pimpl *>(context);
  if (impl == NULL || impl->m_out == NULL || len < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Bad state in gpx_wrap_write."));
  }
  const size_t slen = len;
  if (fwrite(buffer, 1, slen, impl->m_out) < slen) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Failed to write to output stream."));
  }
  return len;
}

static int gpx_wrap_close(void *context) {
  gpx_writer::pimpl *impl = static_cast<gpx_writer::pimpl *>(context);
  if (impl == NULL || impl->m_out == NULL) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Bad state in gpx_wrap_close."));
  }
  if (pclose(impl->m_out) == -1) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Output pipe could not be closed in gpx_wrap_close."));
  }
  impl->m_out = NULL;
  return 0;
}

gpx_writer::pimpl::pimpl(const std::string &file_name, const boost::program_options::variables_map &options)
  : m_command(popen_command(file_name, options)), m_out(popen(m_command.c_str(), "w")), m_writer(NULL) {

  if (m_out == NULL) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to popen compression command for output."));
  }

  xmlOutputBufferPtr output_buffer = xmlOutputBufferCreateIO(gpx_wrap_write, gpx_wrap_close, this, NULL);
  m_writer = xmlNewTextWriter(output_buffer);
  if (m_writer == NULL) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to create xmlTextWriter."));
  }

  xmlTextWriterSetIndent(m_writer, 1);
  if (xmlTextWriterStartDocument(m_writer, NULL, "UTF-8", NULL) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to start document."));
  }
}

void gpx_writer::pimpl::begin(const char *name) {
  if (xmlTextWriterStartElement(m_writer, BAD_CAST name) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to begin element XML."));
  }
}

void gpx_writer::pimpl::end() {
  if (xmlTextWriterEndElement(m_writer) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to end element XML."));
  }
}

void gpx_writer::pimpl::attribute(const char *name, const char *value) {
  if (xmlTextWriterWriteAttribute(m_writer, BAD_CAST name, BAD_CAST value) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to write attribute."));
  }
}

void gpx_writer::pimpl::element(const char *name, const std::string &value) {
  std::string fixed = replace_xml_bad_chars(value);
  if (xmlTextWriterWriteElement(m_writer, BAD_CAST name, BAD_CAST fixed.c_str()) < 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("Unable to write element."));
  }
}

void gpx_writer::pimpl::finish() {
  xmlTextWriterEndDocument(m_writer);
  xmlFreeTextWriter(m_writer);
  m_writer = NULL;
  if (m_out != NULL) {
    pclose(m_out);
    m_out = NULL;
  }
}

gpx_writer::gpx_writer(const std::string &file_name, const boost::program_options::variables_map &options,
                       const pt::ptime &max_time)
  : m_impl(new pimpl(file_name, options)) {

  const std::string generator = options["generator"].as<std::string>();

  m_impl->begin("gpx");
  m_impl->attribute("version", "1.0");
  m_impl->attribute("creator", generator.c_str());
  m_impl->attribute("xmlns", "http://www.topografix.com/GPX/1/0");
  m_impl->attribute("xmlns:osm", OSM_NAMESPACE);

  m_impl->element("author", options["meta-author"].as<std::string>());
  m_impl->element("url", options["meta-copyleft"].as<std::string>());
  if (!max_time.is_special()) {
    m_impl->element("time", fmt_iso_time(max_time));
  }
}

gpx_writer::~gpx_writer() {
}

void gpx_writer::trace(const gpx_file &file, const std::vector<gpx_track> &tracks,
                       const std::vector<std::string> &tags) {
  // decode first, so a broken geometry skips the whole trace and does not
  // leave a half written <trk>.
  std::vector<std::vector<point> > trksegs;
  try {
    int32_t trackid = -1;
    for (size_t i = 0; i < tracks.size(); ++i) {
      // segments split one track by size or distance, and do not share
      // points, so all the segments of a trackid go in one <trkseg>.
      if (trksegs.empty() || tracks[i].trackid != trackid) {
        trksegs.push_back(std::vector<point>());
        trackid = tracks[i].trackid;
      }
      ewkb_reader(tracks[i].geom).read(trksegs.back());
    }
  } catch (const std::exception &e) {
    std::cerr << "WARNING: skipping trace " << file.id << ": " << e.what() << std::endl;
    return;
  }

  m_impl->begin("trk");

  // like the trackpoints API, only identifiable traces have a name and a
  // description. trackable traces are anonymous.
  const bool identifiable = (file.visibility == "identifiable");
  if (identifiable) {
    m_impl->element("name", file.name);
    m_impl->element("desc", file.description);
  }
  m_impl->element("osm:id", (boost::format("%1%") % file.id).str());
  if (identifiable) {
    for (size_t i = 0; i < tags.size(); ++i) {
      m_impl->element("osm:tag", tags[i]);
    }
  }

  char buf[32];
  for (size_t i = 0; i < trksegs.size(); ++i) {
    if (trksegs[i].empty()) {
      continue;
    }
    m_impl->begin("trkseg");
    for (size_t j = 0; j < trksegs[i].size(); ++j) {
      const point &p = trksegs[i][j];
      m_impl->begin("trkpt");
      snprintf(buf, sizeof(buf), "%.7f", p.lat);
      m_impl->attribute("lat", buf);
      snprintf(buf, sizeof(buf), "%.7f", p.lon);
      m_impl->attribute("lon", buf);
      // gpx_tracks saves a missing altitude as 0, so 0 is left out.
      if (p.ele != 0.0) {
        snprintf(buf, sizeof(buf), "%.2f", p.ele);
        m_impl->element("ele", buf);
      }
      // -Infinity is a point without a timestamp.
      if (std::isfinite(p.time)) {
        m_impl->element("time", fmt_epoch(p.time));
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
