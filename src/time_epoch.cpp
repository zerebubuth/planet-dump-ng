#include "time_epoch.hpp"

namespace bt = boost::posix_time;

// set epoch as midnight Jan 1 2004
const bt::ptime time_epoch(boost::gregorian::date(2004, 1, 1), bt::time_duration(0, 0, 0));
