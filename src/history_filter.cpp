#include "history_filter.hpp"
#include <boost/date_time/posix_time/posix_time.hpp>

#include "xml_writer.hpp"
#include "pbf_writer.hpp"

template <typename T>
history_filter<T>::history_filter(const std::string &option_name, const boost::program_options::variables_map &options,
                                  const user_map_t &user_map, const boost::posix_time::ptime &max_time)
  : m_writer(new T(option_name, options, user_map, max_time, false)),
    m_left_over_nodes(boost::none),
    m_left_over_ways(boost::none),
    m_left_over_relations(boost::none) {
}

template <typename T>
history_filter<T>::~history_filter() {
}

template <typename T>
void history_filter<T>::changesets(const std::vector<changeset> &cs, const std::vector<current_tag> &ts) {
  // no filtering for changesets - they are all "current", and all get passed
  // through to the backend.
  m_writer->changesets(cs, ts);
}

template <typename T>
void history_filter<T>::nodes(const std::vector<node> &ns, const std::vector<old_tag> &ts) {
  std::vector<node> cn;
  std::vector<old_tag> ct;

  // handle a left over node, but only if its version list doesn't continue into
  // this block - if it does, then we can ignore the left over one.
  if (m_left_over_nodes && (ns.empty() || (ns[0].id > m_left_over_nodes->n.id))) {
    if (m_left_over_nodes->n.visible) {
      cn.push_back(m_left_over_nodes->n);
      std::swap(m_left_over_nodes->tags, ct);
    }
  }

  std::vector<old_tag>::const_iterator t_itr = ts.begin();
  const std::vector<old_tag>::const_iterator t_end = ts.end();

  for (size_t i = 1; i < ns.size(); ++i) {
    if (ns[i].id > ns[i-1].id) {
      const node &nn = ns[i-1];
      // if the node is deleted, we don't want it in the non-history
      // file, so skip to the next item.
      if (!nn.visible) { continue; }

      cn.push_back(nn);

      while ((t_itr != t_end) && (t_itr->element_id <= nn.id)) {
        if ((t_itr->version == nn.version) && (t_itr->element_id == nn.id)) {
          ct.push_back(*t_itr);
        }
        ++t_itr;
      }
    }
  }

  // push to the underlying writer
  m_writer->nodes(cn, ct);

  // and save the last node for next time
  if (!ns.empty()) {
    if (!m_left_over_nodes) { m_left_over_nodes = left_over_nodes(); }
    const node &nn = ns[ns.size()-1];
    m_left_over_nodes->n = nn;
    m_left_over_nodes->tags.clear();

    while ((t_itr != t_end) && (t_itr->element_id <= nn.id)) {
      if ((t_itr->version == nn.version) && (t_itr->element_id == nn.id)) {
        m_left_over_nodes->tags.push_back(*t_itr);
      }
      ++t_itr;
    }
  } else {
    m_left_over_nodes = boost::none;
  }
}

template <typename T>
void history_filter<T>::ways(const std::vector<way> &ws, const std::vector<way_node> &wns, const std::vector<old_tag> &ts) {
  std::vector<way> cw;
  std::vector<way_node> cwn;
  std::vector<old_tag> ct;

  // if there are any left over nodes, finish them now
  if (m_left_over_nodes) {
    std::vector<node> ns; std::vector<old_tag> nts;
    nodes(ns, nts);
  }

  // handle a left over way, but only if its version list doesn't continue into
  // this block - if it does, then we can ignore the left over one.
  if (m_left_over_ways && (ws.empty() || (ws[0].id > m_left_over_ways->w.id))) {
    if (m_left_over_ways->w.visible) {
      cw.push_back(m_left_over_ways->w);
      std::swap(m_left_over_ways->nodes, cwn);
      std::swap(m_left_over_ways->tags, ct);
    }
  }

  std::vector<way_node>::const_iterator n_itr = wns.begin();
  const std::vector<way_node>::const_iterator n_end = wns.end();
  std::vector<old_tag>::const_iterator t_itr = ts.begin();
  const std::vector<old_tag>::const_iterator t_end = ts.end();

  for (size_t i = 1; i < ws.size(); ++i) {
    if (ws[i].id > ws[i-1].id) {
      const way &ww = ws[i-1];
      // if the way is deleted, we don't want it in the non-history
      // file, so skip to the next item.
      if (!ww.visible) { continue; }

      cw.push_back(ww);

      while ((n_itr != n_end) && (n_itr->way_id <= ww.id)) {
        if ((n_itr->version == ww.version) && (n_itr->way_id == ww.id)) {
          cwn.push_back(*n_itr);
        }
        ++n_itr;
      }

      while ((t_itr != t_end) && (t_itr->element_id <= ww.id)) {
        if ((t_itr->version == ww.version) && (t_itr->element_id == ww.id)) {
          ct.push_back(*t_itr);
        }
        ++t_itr;
      }
    }
  }

  // push to the underlying writer
  m_writer->ways(cw, cwn, ct);

  // and save the last way for next time
  if (!ws.empty()) {
    if (!m_left_over_ways) { m_left_over_ways = left_over_ways(); }
    const way &ww = ws[ws.size()-1];
    m_left_over_ways->w = ww;
    m_left_over_ways->nodes.clear();
    m_left_over_ways->tags.clear();

    while ((n_itr != n_end) && (n_itr->way_id <= ww.id)) {
      if ((n_itr->version == ww.version) && (n_itr->way_id == ww.id)) {
        m_left_over_ways->nodes.push_back(*n_itr);
      }
      ++n_itr;
    }

    while ((t_itr != t_end) && (t_itr->element_id <= ww.id)) {
      if ((t_itr->version == ww.version) && (t_itr->element_id == ww.id)) {
        m_left_over_ways->tags.push_back(*t_itr);
      }
      ++t_itr;
    }
  } else {
    m_left_over_ways = boost::none;
  }
}

template <typename T>
void history_filter<T>::relations(const std::vector<relation> &rs, const std::vector<relation_member> &rms, const std::vector<old_tag> &ts) {
  std::vector<relation> cr;
  std::vector<relation_member> crm;
  std::vector<old_tag> ct;

  // if there are any ways left over, finish them now
  if (m_left_over_ways) {
    std::vector<way> ws; std::vector<way_node> wns; std::vector<old_tag> wts;
    ways(ws, wns, wts);
  }

  // handle a left over relation, but only if its version list doesn't continue into
  // this block - if it does, then we can ignore the left over one.
  if (m_left_over_relations && (rs.empty() || (rs[0].id > m_left_over_relations->r.id))) {
    if (m_left_over_relations->r.visible) {
      cr.push_back(m_left_over_relations->r);
      std::swap(m_left_over_relations->members, crm);
      std::swap(m_left_over_relations->tags, ct);
    }
  }

  std::vector<relation_member>::const_iterator m_itr = rms.begin();
  const std::vector<relation_member>::const_iterator m_end = rms.end();
  std::vector<old_tag>::const_iterator t_itr = ts.begin();
  const std::vector<old_tag>::const_iterator t_end = ts.end();

  for (size_t i = 1; i < rs.size(); ++i) {
    if (rs[i].id > rs[i-1].id) {
      const relation &rr = rs[i-1];
      // if the relation is deleted, we don't want it in the non-history
      // file, so skip to the next item.
      if (!rr.visible) { continue; }

      cr.push_back(rr);

      while ((m_itr != m_end) && (m_itr->relation_id <= rr.id)) {
        if ((m_itr->version == rr.version) && (m_itr->relation_id == rr.id)) {
          crm.push_back(*m_itr);
        }
        ++m_itr;
      }

      while ((t_itr != t_end) && (t_itr->element_id <= rr.id)) {
        if ((t_itr->version == rr.version) && (t_itr->element_id == rr.id)) {
          ct.push_back(*t_itr);
        }
        ++t_itr;
      }
    }
  }

  // push to the underlying writer
  m_writer->relations(cr, crm, ct);

  // and save the last relation for next time
  if (!rs.empty()) {
    if (!m_left_over_relations) { m_left_over_relations = left_over_relations(); }
    const relation &rr = rs[rs.size()-1];
    m_left_over_relations->r = rr;
    m_left_over_relations->members.clear();
    m_left_over_relations->tags.clear();

    while ((m_itr != m_end) && (m_itr->relation_id <= rr.id)) {
      if ((m_itr->version == rr.version) && (m_itr->relation_id == rr.id)) {
        m_left_over_relations->members.push_back(*m_itr);
      }
      ++m_itr;
    }

    while ((t_itr != t_end) && (t_itr->element_id <= rr.id)) {
      if ((t_itr->version == rr.version) && (t_itr->element_id == rr.id)) {
        m_left_over_relations->tags.push_back(*t_itr);
      }
      ++t_itr;
    }
  } else {
    m_left_over_relations = boost::none;
  }
}

template <typename T>
void history_filter<T>::finish() {
  // if there are any left over relations, finish them now.
  if (m_left_over_relations) {
    std::vector<relation> rs; std::vector<relation_member> rms; std::vector<old_tag> rts;
    relations(rs, rms, rts);
  }

  // finish the underlying output writer
  m_writer->finish();
}

template struct history_filter<xml_writer>;
template struct history_filter<pbf_writer>;
