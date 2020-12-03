#include "copy_elements.hpp"
#include "dump_archive.hpp"
#include "output_writer.hpp"
#include "xml_writer.hpp"
#include "pbf_writer.hpp"
#include "history_filter.hpp"
#include "changeset_filter.hpp"
#include "config.h"
#include "writer_common.hpp"

#include <boost/shared_ptr.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/make_shared.hpp>

#include <boost/foreach.hpp>
#include <string>
#include <map>
#include <fstream>
#include <stdexcept>

namespace bt = boost::posix_time;
namespace po = boost::program_options;

/**
 * get command line options, handle --help and usage, validate options.
 */
static void get_options(int argc, char **argv, po::variables_map &vm) {
  std::string meta_file;

  po::options_description desc(PACKAGE_STRING ": Allowed options");

  desc.add_options()
    ("help,h", "display help text and exit")
    ("compress-command,c", po::value<std::string>()->default_value("bzip2 -c"),
     "program used to compress XML output, must read from stdin and write to stdout")
    ("xml,x", po::value<std::string>(), "planet XML output file (without history)")
    ("history-xml,X", po::value<std::string>(), "history XML output file")
    ("pbf,p", po::value<std::string>(), "planet PBF output file (without history)")
    ("history-pbf,P", po::value<std::string>(), "history PBF output file")
    ("changesets,C", po::value<std::string>(), "changeset XML output file")
    ("changeset-discussions,D", po::value<std::string>(),
     "changeset discussions XML output file")
    ("xml-no-userinfo", po::value<std::string>(), "planet XML output file (without history or user data)")
    ("history-xml-no-userinfo", po::value<std::string>(), "history XML output file (without user data)")
    ("pbf-no-userinfo", po::value<std::string>(), "planet PBF output file (without history or user data)")
    ("history-pbf-no-userinfo", po::value<std::string>(), "history PBF output file (without user data)")
    ("changesets-no-userinfo", po::value<std::string>(), "changeset XML output file (without user data)")
    ("changeset-discussions-no-userinfo", po::value<std::string>(),
     "changeset discussions XML output file (without user data)")
    ("dense-nodes,d", po::value<bool>()->default_value("true"), "use dense nodes for PBF output")
    ("dump-file,f", po::value<std::string>(), "PostgreSQL table dump to read")
    ("generator", po::value<std::string>()->default_value(PACKAGE_STRING),
     "Override the generator string used by the program. Used by the tests to "
     "ensure consistent output, probably shouldn't be used in normal usage.")
    ("resume", "If this argument is present, then planet-dump-ng will attempt "
     "to resume processing from partial data. If not present, then it will "
     "start from scratch.")
    ("max-concurrency", po::value<unsigned int>()->default_value(16),
      "Maximum number of disk writing threads to run for *each* table.")
    ("meta-file,M", po::value<std::string>(&meta_file), "data metainfo configuration file")
    ;
    
  po::options_description meta;
  
  meta.add_options()
    ("meta-author", po::value<std::string>()->default_value(OSM_COPYRIGHT_TEXT), "author data metainfo")
    ("meta-source", po::value<std::string>()->default_value(OSM_API_ORIGIN), "source data metainfo")
    ("meta-copyleft", po::value<std::string>()->default_value(OSM_LICENSE_TEXT), "copyleft data metainfo")
    ("meta-attribution", po::value<std::string>()->default_value(OSM_ATTRIBUTION_TEXT), "attribution data metainfo")
    ;
  
  po::options_description optns;
  optns.add(desc).add(meta);

  po::store(po::parse_command_line(argc, argv, optns), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    exit(0);
  }

  if (vm.count("dump-file") == 0) {
    BOOST_THROW_EXCEPTION(std::runtime_error("A PostgreSQL table dump file (--dump-file) must be provided."));
  }

  if ((vm.count("xml") + vm.count("history-xml") +
       vm.count("pbf") + vm.count("history-pbf") + 
       vm.count("changesets") + vm.count("changeset-discussions") +
       vm.count("xml-no-userinfo") + vm.count("history-xml-no-userinfo") +
       vm.count("pbf-no-userinfo") + vm.count("history-pbf-no-userinfo") + 
       vm.count("changesets-no-userinfo") + vm.count("changeset-discussions-no-userinfo")) == 0) {
    std::cerr <<
      "No output file provided! You must provide one or more of "
      "--xml, --history-xml, --pbf, --history-pbf, --changesets, "
      "--changeset-discussions (or the respective -no-userinfo options) to get output.\n\n";
    std::cerr << desc << std::endl;
    exit(1);
  }
  
  if (vm.count("meta-file")) {
    std::ifstream ifs(meta_file.c_str());
    if (!ifs)
    {
      std::cout << "Can not open metainfo file: " << meta_file << "\n";
      exit(1);
    }
    else
    {
      po::store(po::parse_config_file(ifs, optns), vm);
      po::notify(vm);
    }
  }
}

/**
 * read the dump file in parallel to get all of the elements into on-disk
 * databases. this is primarily so that the data is sorted, which is not
 * guaranteed in the PostgreSQL dump file. returns the maximum time seen
 * in a timestamp of any element in the dump file.
 */
bt::ptime setup_databases(const std::string &dump_file, bool resume, unsigned int max_concurrency) {
  std::list<boost::shared_ptr<base_thread> > threads;
  
#define THREAD_RUN(type,table) threads.push_back(boost::make_shared<run_thread<type> >(table, dump_file, resume, max_concurrency))

  THREAD_RUN(changeset, "changesets");
  THREAD_RUN(node, "nodes");
  THREAD_RUN(way, "ways");
  THREAD_RUN(relation, "relations");
  
  THREAD_RUN(current_tag, "changeset_tags");
  THREAD_RUN(old_tag, "node_tags");
  THREAD_RUN(old_tag, "way_tags");
  THREAD_RUN(old_tag, "relation_tags");
  THREAD_RUN(way_node, "way_nodes");
  THREAD_RUN(relation_member, "relation_members");
  
  THREAD_RUN(user, "users");
  THREAD_RUN(changeset_comment, "changeset_comments");

#undef THREAD_RUN
  
  bt::ptime max_time(bt::neg_infin);
  BOOST_FOREACH(boost::shared_ptr<base_thread> &thr, threads) {
    max_time = std::max(max_time, thr->join());
    thr.reset();
  }
  threads.clear();

  return max_time;
}

int main(int argc, char *argv[]) {
  try {
    po::variables_map options;
    get_options(argc, argv, options);

    // workaround for https://svn.boost.org/trac/boost/ticket/5638
    boost::gregorian::greg_month::get_month_map_ptr();

    // extract data from the dump file for the "sorted" data tables, like nodes,
    // ways, relations, changesets and their associated tags, etc...
    const bool resume = options.count("resume") > 0;
    unsigned int max_concurrency = options["max-concurrency"].as<unsigned int>();
    const std::string dump_file(options["dump-file"].as<std::string>());
    const bt::ptime max_time = setup_databases(dump_file, resume, max_concurrency);

    // users aren't dumped directly to the files. we only use them to build up a map
    // of uid -> name where a missing uid indicates that the user doesn't have public
    // data.
    std::map<int64_t, std::string> display_name_map;
    extract_users(display_name_map);

    // build up a list of writers. these will be written to in parallel, which is
    // mildly wasteful if there's just one output type, but works great when all of
    // the output types are being used.
    std::vector<boost::shared_ptr<output_writer> > writers;
    if (options.count("history-xml")) {
      std::string output_file = options["history-xml"].as<std::string>();
      writers.push_back(boost::shared_ptr<output_writer>(new xml_writer(output_file, options, 
        display_name_map, max_time, user_info_level::FULL, historical_versions::FULL, changeset_discussions::NONE)));
    }
    if (options.count("history-xml-no-userinfo")) {
      std::string output_file = options["history-xml-no-userinfo"].as<std::string>();
      writers.push_back(boost::shared_ptr<output_writer>(new xml_writer(output_file, options, 
        display_name_map, max_time, user_info_level::ANON, historical_versions::FULL, changeset_discussions::NONE)));
    }
    if (options.count("history-pbf")) {
      std::string output_file = options["history-pbf"].as<std::string>();
      writers.push_back(boost::shared_ptr<output_writer>(new pbf_writer(output_file, options, 
        display_name_map, max_time, user_info_level::FULL, historical_versions::FULL, changeset_discussions::NONE)));
    }
    if (options.count("history-pbf-no-userinfo")) {
      std::string output_file = options["history-pbf-no-userinfo"].as<std::string>();
      writers.push_back(boost::shared_ptr<output_writer>(new pbf_writer(output_file, options, 
        display_name_map, max_time, user_info_level::ANON, historical_versions::FULL, changeset_discussions::NONE)));
    }
    if (options.count("xml")) {
      std::string output_file = options["xml"].as<std::string>();
      writers.push_back(boost::shared_ptr<output_writer>(new history_filter<xml_writer>(output_file, options, 
        display_name_map, max_time, user_info_level::FULL, historical_versions::NONE, changeset_discussions::NONE)));
    }
    if (options.count("xml-no-userinfo")) {
      std::string output_file = options["xml-no-userinfo"].as<std::string>();
      writers.push_back(boost::shared_ptr<output_writer>(new history_filter<xml_writer>(output_file, options, 
        display_name_map, max_time, user_info_level::ANON, historical_versions::NONE, changeset_discussions::NONE)));
    }
    if (options.count("pbf")) {
      std::string output_file = options["pbf"].as<std::string>();
      writers.push_back(boost::shared_ptr<output_writer>(new history_filter<pbf_writer>(output_file, options, 
        display_name_map, max_time, user_info_level::FULL, historical_versions::NONE, changeset_discussions::NONE)));
    }
    if (options.count("pbf-no-userinfo")) {
      std::string output_file = options["pbf-no-userinfo"].as<std::string>();
      writers.push_back(boost::shared_ptr<output_writer>(new history_filter<pbf_writer>(output_file, options, 
        display_name_map, max_time, user_info_level::ANON, historical_versions::NONE, changeset_discussions::NONE)));
    }
    if (options.count("changesets")) {
      std::string output_file = options["changesets"].as<std::string>();
      writers.push_back(boost::shared_ptr<output_writer>(new changeset_filter<xml_writer>(output_file, options, 
        display_name_map, max_time, user_info_level::FULL, historical_versions::NONE, changeset_discussions::NONE)));
    }
    if (options.count("changesets-no-userinfo")) {
      std::string output_file = options["changesets-no-userinfo"].as<std::string>();
      writers.push_back(boost::shared_ptr<output_writer>(new changeset_filter<xml_writer>(output_file, options, 
        display_name_map, max_time, user_info_level::ANON, historical_versions::NONE, changeset_discussions::NONE)));
    }
    if (options.count("changeset-discussions")) {
      std::string output_file = options["changeset-discussions"].as<std::string>();
      writers.push_back(boost::shared_ptr<output_writer>(new changeset_filter<xml_writer>(output_file, options, 
        display_name_map, max_time, user_info_level::FULL, historical_versions::NONE, changeset_discussions::FULL)));
    }
    if (options.count("changeset-discussions-no-userinfo")) {
      std::string output_file = options["changeset-discussions-no-userinfo"].as<std::string>();
      writers.push_back(boost::shared_ptr<output_writer>(new changeset_filter<xml_writer>(output_file, options, 
        display_name_map, max_time, user_info_level::ANON, historical_versions::NONE, changeset_discussions::FULL)));
    }

    std::cerr << "Writing changesets..." << std::endl;
    run_threads<changeset>(writers);
    std::cerr << "Writing nodes..." << std::endl;
    run_threads<node>(writers);
    std::cerr << "Writing ways..." << std::endl;
    run_threads<way>(writers);
    std::cerr << "Writing relations..." << std::endl;
    run_threads<relation>(writers);

    // tell writers to clean up - write finals, close files, that sort of thing
    BOOST_FOREACH(boost::shared_ptr<output_writer> writer, writers) {
      writer->finish();
    }
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
