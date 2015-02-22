Planet Dump (Next Generation)
=============================

Experimental next version of the planet dump tool for OpenStreetMap.

This planet dump program operates in a very different way from the
previous one; rather than force the database server to keep a
consistent context open for the duration of the dump, it instead uses
a consistent dump from the database. This means that running the
extraction from PostgreSQL dump file to planet file(s) is completely
independent of the database server, and can be done on a disconnected
machine without putting any load on any database.

Building
--------

Before building the code, you will need:

* A C++ build system (GCC 4.7 recommended),
* libxml2 (version 2.6.31 recommended),
* The Boost libraries (version 1.49 recommended),
* libosmpbf (version 1.3.0 recommended),
* libprotobuf and libprotobuf-lite (version 2.4.1 recommended)

To install these on Ubuntu, you can just type:

    sudo apt-get install build-essential automake autoconf \
      libxml2-dev libboost-dev libboost-program-options-dev \
      libboost-date-time-dev libboost-filesystem-dev \
      libboost-thread-dev libboost-iostreams-dev \
      libosmpbf-dev osmpbf-bin libprotobuf-dev pkg-config

After that, it should just be a matter of running:

    ./autogen.sh
    ./configure
    make
  
If you run into any issues with this, please file a bug on the github
issues page for this project, giving as much detail as you can about
the error and the environment it occurred in.

Running
-------

The planet dump program has a decent built-in usage description, which
you can read by running:

    planet-dump-ng --help

One thing to note is that the program will create on-disk databases in
the current working directory, so it is wise to run the program
somewhere with plenty of fast disk space. Existing files may interfere
with the operation of the program, so it's best to run it in its own,
clean directory.

Architecture
------------

This started out with the aim of being easy to change in response to
schema changes in the API. However, somehow the templates escaped and
began to multiply. Sadly, the code is now much less readable than I
would like, but on the bright side is a contender for the Most
Egregiously Templated Code award.

Simplifying, the code consists of two basic parts; the bit which reads
the PostgreSQL dump, and the part which writes XML and/or PBF.

The part which reads the PostgreSQL dump operates by launching
"pg_restore" as a sub-process and parsing its output (in quite a naive
way) to get the row data. The part which writes the XML and/or PBF then
does a join between the top level elements like nodes, ways and
relations and their "inners" - things like tags, way nodes and relation
members.

In order that the system can output a planet file or a history planet
file in the same run, all of this is generated from the history
tables. This means a minor adjustment to how the "current" planet is
written, with a filter which drops any non-current version of an
element and any current version which is deleted.

History
-------

This evolved, by a somewhat roundabout route, from an attempt to
create a new planet dump which read the absolute minimum from the
database; that is changesets, changeset tags and just the IDs and
versions of the current tables for nodes, ways and relations. The
remaining information could be filled in at any time from the history
tables because, with the minor exception of redactions, the nodes,
ways and relations tables are append-only.

Dumping the IDs and versions would still take time, so it seemed worth
looking at "pg_dump" to see how it would best be done
efficiently. While looking at "pg_dump", it became clear that what was
really needed was just the dump itself - a dump which is produced
regularly for backup purposes anyway.

