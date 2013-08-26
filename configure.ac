AC_INIT([planet-dump-ng],
	[0.0.1],
	[https://github.com/zerebubuth/openstreetmap-planet-dump-ng/issues],
	[planet-dump-ng-0.0.1],
	[https://github.com/zerebubuth/openstreetmap-planet-dump-ng])
AM_INIT_AUTOMAKE([subdir-objects parallel-tests])
AM_SILENT_RULES([yes])
AC_CONFIG_HEADERS([include/config.h])

AC_PROG_CXX
AC_PROG_INSTALL
AC_PROG_MAKE_SET

AC_HEADER_STDC
AC_LANG_CPLUSPLUS

PKG_CHECK_MODULES(LIBXML, libxml-2.0 >= 2.6.31)
AC_SUBST(LIBXML_CFLAGS)
AC_SUBST(LIBXML_LIBS)

AX_BOOST_BASE([1.43], , [AC_MSG_ERROR([Boost libraries are required for building CGImap. Please install libboost-dev.])])
AX_BOOST_SYSTEM
AX_BOOST_FILESYSTEM
AX_BOOST_PROGRAM_OPTIONS
AX_BOOST_DATE_TIME

AC_ARG_WITH(leveldb,
	[AS_HELP_STRING([--with-leveldb=<path>], [Path containing LEVELDB library (e.g: /usr).])],
	[LEVELDB_PREFIX=$with_leveldb],
	AC_MSG_ERROR([You must call configure with the --with-leveldb option.
	This tells configure where to find the LEVELDB library and headers.
	e.g. --with-leveldb=/usr/local or --with-leveldb=/usr])
)

AC_SUBST(LEVELDB_PREFIX)
LEVELDB_LIBS="-L${LEVELDB_PREFIX}/lib -lleveldb -lsnappy"
LEVELDB_CFLAGS="-I${LEVELDB_PREFIX}/include"
AC_SUBST(LEVELDB_LIBS)
AC_SUBST(LEVELDB_CFLAGS)

AC_CONFIG_FILES([
	Makefile
	src/Makefile])

AC_OUTPUT
