#include "time_epoch.hpp"
#include <cstdlib>
#include <string>

namespace bt = boost::posix_time;
namespace gr = boost::gregorian;

/**
 * Reads the PLANET_EPOCH_DATE environment variable if set.
 *
 * This variable allows overriding the default epoch date (January 1, 2004),
 * which is used for tagging or metadata generation in output files.
 *
 * Expected format: YYYY-MM-DD (e.g., 2025-06-30)
 *
 * If the variable is not set or invalid, the default epoch will be used.
 */
gr::date get_epoch_date() {
    const char* env = std::getenv("PLANET_EPOCH_DATE");
    if (env) {
        try {
            return gr::from_simple_string(env);
        } catch (...) {
            std::cerr << "Invalid PLANET_EPOCH_DATE format. Using default epoch." << std::endl;
        }
    }
    // set epoch as midnight Jan 1 2004
    return gr::date(2004, 1, 1);
}

const bt::ptime time_epoch(get_epoch_date(), bt::time_duration(0, 0, 0));
