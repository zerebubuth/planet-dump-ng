#include "history_filter.hpp"
#include <boost/date_time/posix_time/posix_time.hpp>

#include "xml_writer.hpp"
#include "pbf_writer.hpp"

template <typename T>
history_filter<T>::history_filter(const std::string &option_name, const boost::program_options::variables_map &options,
                                  const user_map_t &user_map, const boost::posix_time::ptime &max_time)
  : m_writer(new T(option_name, options, user_map, max_time, true)) {
}

template <typename T>
history_filter<T>::~history_filter() {
}

template <typename T>
void history_filter<T>::nodes(const std::vector<node> &, const std::vector<old_tag> &) {
}

template <typename T>
void history_filter<T>::ways(const std::vector<way> &, const std::vector<way_node> &, const std::vector<old_tag> &) {
}

template <typename T>
void history_filter<T>::relations(const std::vector<relation> &, const std::vector<relation_member> &, const std::vector<old_tag> &) {
}

template <typename T>
void history_filter<T>::finish() {
}

template struct history_filter<xml_writer>;
template struct history_filter<pbf_writer>;
