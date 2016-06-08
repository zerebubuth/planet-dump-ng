AC_INIT([planet-dump-ng],
	[1.1.4],
	[https://github.com/zerebubuth/openstreetmap-planet-dump-ng/issues],
	[planet-dump-ng-1.1.4],
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

AX_BOOST_BASE([1.43], , [AC_MSG_ERROR([Boost libraries are required for building planet-dump-ng. Please install libboost-dev.])])
AX_BOOST_SYSTEM
AX_BOOST_FILESYSTEM
AX_BOOST_PROGRAM_OPTIONS
AX_BOOST_DATE_TIME
AX_BOOST_THREAD
AX_BOOST_IOSTREAMS

PKG_CHECK_MODULES([PROTOBUF_LITE], "protobuf-lite")
AC_SUBST([PROTOBUF_LITE_CFLAGS])
AC_SUBST([PROTOBUF_LITE_LIBS])

PKG_CHECK_MODULES([PROTOBUF], "protobuf")
AC_SUBST([PROTOBUF_CFLAGS])
AC_SUBST([PROTOBUF_LIBS])

AC_CHECK_HEADER([osmpbf/osmpbf.h],[],[AC_MSG_ERROR([Unable to find the osmpbf headers, you might need to install libosmpbf-dev.])])

AC_MSG_CHECKING([whether you have an ancient version of osmpbf.])
AC_ARG_ENABLE([old-osmpbf],
	[AS_HELP_STRING([--enable-old-osmpbf],
                [Use this flag if you have a version of libosmpbf-dev before 1.3.0.])],
        [with_old_osmpbf="yes"],
        [with_old_osmpbf="no"])
AC_MSG_RESULT($with_old_osmpbf)
AS_IF([test "x$with_old_osmpbf" == xyes],
        [AC_DEFINE([WITH_OLD_OSMPBF], [1], [Define when libosmpbf version is ancient.])])
AM_CONDITIONAL([WITH_OLD_OSMPBF], [test "x$with_old_osmpbf" == xyes])

AC_CONFIG_FILES([
	Makefile
	src/Makefile])

AC_OUTPUT
