#include "copy_elements.hpp"
#include "dump_archive.hpp"
#include "output_writer.hpp"
#include "xml_writer.hpp"
#include "pbf_writer.hpp"

#include <boost/shared_ptr.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/make_shared.hpp>

#include <boost/foreach.hpp>
#include <string>
#include <map>
#include <fstream>
#include <stdexcept>

namespace bt = boost::posix_time;

int main(int argc, char *argv[]) {
  try {
    if (argc != 3) {
       throw std::runtime_error("Usage: ./planet-dump <file> <pbf-file>");
    }

    // workaround for https://svn.boost.org/trac/boost/ticket/5638
    boost::gregorian::greg_month::get_month_map_ptr();

    std::string dump_file(argv[1]);
    std::map<int64_t, std::string> display_name_map;

    std::list<boost::shared_ptr<base_thread> > threads;

    threads.push_back(boost::make_shared<run_thread<changeset> >("changesets", dump_file));
    threads.push_back(boost::make_shared<run_thread<node> >("nodes", dump_file));
    threads.push_back(boost::make_shared<run_thread<way> >("ways", dump_file));
    threads.push_back(boost::make_shared<run_thread<relation> >("relations", dump_file));

    threads.push_back(boost::make_shared<run_thread<current_tag> >("changeset_tags", dump_file));
    threads.push_back(boost::make_shared<run_thread<old_tag> >("node_tags", dump_file));
    threads.push_back(boost::make_shared<run_thread<old_tag> >("way_tags", dump_file));
    threads.push_back(boost::make_shared<run_thread<old_tag> >("relation_tags", dump_file));
    threads.push_back(boost::make_shared<run_thread<way_node> >("way_nodes", dump_file));
    threads.push_back(boost::make_shared<run_thread<relation_member> >("relation_members", dump_file));

    threads.push_back(boost::make_shared<run_thread<user> >("users", dump_file));

    bt::ptime max_time(bt::neg_infin);
    BOOST_FOREACH(boost::shared_ptr<base_thread> &thr, threads) {
      max_time = std::max(max_time, thr->join());
      thr.reset();
    }
    threads.clear();

    extract_users(dump_file, display_name_map);
    std::ofstream pbf_out(argv[2]);
    std::vector<boost::shared_ptr<output_writer> > writers;
    writers.push_back(boost::shared_ptr<output_writer>(new pbf_writer(pbf_out, display_name_map, max_time)));
    writers.push_back(boost::shared_ptr<output_writer>(new xml_writer(std::cout, display_name_map, max_time)));

    std::cerr << "Writing changesets..." << std::endl;
    //extract_changesets(dump_file, writer);
    std::cerr << "Writing nodes..." << std::endl;
    run_threads<node>(dump_file, writers);
    std::cerr << "Writing ways..." << std::endl;
    run_threads<way>(dump_file, writers);
    std::cerr << "Writing relations..." << std::endl;
    run_threads<relation>(dump_file, writers);
    std::cerr << "Done" << std::endl;

  } catch (const boost::exception &e) {
    std::cerr << "EXCEPTION: " << boost::current_exception_diagnostic_information() << "\n";
    return 1;

  } catch (const std::exception &e) {
    std::cerr << "EXCEPTION: " << boost::current_exception_diagnostic_information() << "\n";
    return 1;

  } catch (...) {
    std::cerr << "UNEXPLAINED ERROR\n";
    return 1;
  }

  return 0;
}
