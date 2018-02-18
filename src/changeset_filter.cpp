
#include "changeset_filter.hpp"
#include <boost/date_time/posix_time/posix_time.hpp>

#include "xml_writer.hpp"

template <typename T>
changeset_filter<T>::changeset_filter(const std::string &option_name, const boost::program_options::variables_map &options,
                                      const user_map_t &user_map, const boost::posix_time::ptime &max_time, bool clean,
                                      bool include_discussions)
  : m_writer(new T(option_name, options, user_map, max_time, clean, false, include_discussions)) {
}

template <typename T>
changeset_filter<T>::~changeset_filter() {
}

template <typename T>
void changeset_filter<T>::changesets(const std::vector<changeset> &cs,
                                     const std::vector<current_tag> &ts,
                                     const std::vector<changeset_comment> &ccs) {
  // no filtering for changesets - we want them.
  m_writer->changesets(cs, ts, ccs);
}

template <typename T>
void changeset_filter<T>::nodes(const std::vector<node> &ns, const std::vector<old_tag> &ts) {
  // do nothing - we don't want nodes in the changeset output
}

template <typename T>
void changeset_filter<T>::ways(const std::vector<way> &ws, const std::vector<way_node> &wns, const std::vector<old_tag> &ts) {
  // do nothing - we don't want ways in the changeset output
}

template <typename T>
void changeset_filter<T>::relations(const std::vector<relation> &rs, const std::vector<relation_member> &rms, const std::vector<old_tag> &ts) {
  // do nothing - we don't want relations in the changeset output
}

template <typename T>
void changeset_filter<T>::finish() {
  // finish the underlying output writer
  m_writer->finish();
}

// note that a changeset_filter on pbf_writer is, at present, 
// somewhat useless due to the lack of changeset implementation
// in PBF format.
template struct changeset_filter<xml_writer>;
