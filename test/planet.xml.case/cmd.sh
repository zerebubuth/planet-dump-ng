#!/bin/bash

$1/planet-dump-ng --generator "planet-dump-ng test X.Y.Z" --xml planet.osm.bz2 --xml-no-userinfo planet-no-userinfo.osm.pbz2 --dump-file $1/test/liechtenstein-2013-08-03.dmp
