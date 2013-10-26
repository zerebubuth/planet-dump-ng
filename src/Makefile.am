LDADD=@LEVELDB_LIBS@ @LIBXML_LIBS@ @BOOST_FILESYSTEM_LIB@ @BOOST_PROGRAM_OPTIONS_LIB@ @BOOST_DATE_TIME_LIB@ @BOOST_SYSTEM_LIB@ @BOOST_THREAD_LIB@ @PROTOBUF_LITE_LIBS@ @PROTOBUF_LIBS@ -losmpbf
AM_LDFLAGS=@BOOST_LDFLAGS@
AM_CPPFLAGS=-I../include @LEVELDB_CFLAGS@ @LIBXML_CFLAGS@ @BOOST_CPPFLAGS@ @PROTOBUF_LITE_CFLAGS@ @PROTOBUF_CFLAGS@

bin_PROGRAMS=../planet-dump-ng
################################################################################
___planet_dump_ng_SOURCES=\
	copy_elements.cpp \
	dump_archive.cpp \
	dump_reader.cpp \
	extract_kv.cpp \
	history_filter.cpp \
	insert_kv.cpp \
	output_writer.cpp \
	pbf_writer.cpp \
	planet-dump.cpp \
	time_epoch.cpp \
	types.cpp \
	xml_writer.cpp

