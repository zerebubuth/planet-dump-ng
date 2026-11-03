#include "writer_common.hpp"

#include <sstream>
#include <stdexcept>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/exception/all.hpp>

namespace pt = boost::posix_time;

namespace {

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

} // anonymous namespace

/**
 * according to http://www.w3.org/TR/xml/#charsets, there are a range of
 * characters which are valid UTF-8, but invalid XML. we remove some of
 * them here, mainly ASCII control characters, which otherwise choke
 * something which would read these files. note that libxml2 will
 * happily *output* them, which is also a problem for anyone reading
 * from the API :-(
 */
std::string replace_xml_bad_chars(const std::string &s) {
  const size_t size = s.size();
  std::string output(size, '\0');

  for (size_t i = 0; i < size; ++i) {
    char c = s[i];
    if ((c >= 0x00) && (c < 0x20) && (c != 0x09) && (c != 0x0a) && (c != 0x0d)) {
      // replace with question mark is same behaviour as existing planet
      // dump program.
      c = '?';
    }
    output[i] = c;
  }

  return output;
}

std::string popen_command(const std::string &file_name, const boost::program_options::variables_map &options) {
  std::string compress_command;
  try {
    compress_command = options["compress-command"].as<std::string>();
  } catch (...) {
    boost::throw_exception(
      boost::enable_error_info(
        std::runtime_error((boost::format("Unable to get options for \"%1%\".") % file_name).str()))
      << boost::errinfo_nested_exception(boost::current_exception()));
  }

  // need to shell escape the file name.
  // NOTE: this seems to be incredibly ill-defined, and varies depending on the
  // system shell. a better way would be to open the file directly and dup
  // the file descriptor, but that seems to be quite a pain in the arse.
  std::string escaped_file_name(file_name);
  boost::find_format_all(escaped_file_name, boost::token_finder(boost::is_any_of("\\\"")), shell_escape_char());

  std::ostringstream command;
  command << compress_command << " > \"" << escaped_file_name << "\"";
  return command.str();
}

// profiling revealed that conversion to a time was a hotspot in the
// code - not the conversion itself, but the allocation and setup of
// the locale objects used to do the formatting. since we want an
// ISO standard string, in zulu time, always then we don't need any
// of that overhead.
std::string fmt_iso_time(const pt::ptime &t) {
  std::string s;
  if (!t.is_special()) {
    //           00000000001111111111
    //           01234567890123456789
    // format is YYYY-mm-ddTHH:MM:SSZ
    s.resize(21);

    const boost::gregorian::greg_year_month_day ymd = t.date().year_month_day();
    const pt::time_duration tod = t.time_of_day();
    const long hour = tod.hours();
    const long minute = tod.minutes();
    const long second = tod.seconds();

    s[ 0] = '0' + ((ymd.year / 1000) % 10);
    s[ 1] = '0' + ((ymd.year /  100) % 10);
    s[ 2] = '0' + ((ymd.year /   10) % 10);
    s[ 3] = '0' + ((ymd.year       ) % 10);
    s[ 4] = '-';
    s[ 5] = (ymd.month >= 10) ? '1' : '0';
    s[ 6] = '0' + (ymd.month % 10);
    s[ 7] = '-';
    s[ 8] = '0' + ((ymd.day / 10) % 10);
    s[ 9] = '0' + (ymd.day % 10);
    s[10] = 'T';
    s[11] = '0' + ((hour / 10) % 10);
    s[12] = '0' + (hour % 10);
    s[13] = ':';
    s[14] = '0' + ((minute / 10) % 10);
    s[15] = '0' + (minute % 10);
    s[16] = ':';
    s[17] = '0' + ((second / 10) % 10);
    s[18] = '0' + (second % 10);
    s[19] = 'Z';
    s[20] = '\0';
  }
  return s;
}
