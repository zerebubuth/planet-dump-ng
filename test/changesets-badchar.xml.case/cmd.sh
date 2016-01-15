#!/bin/bash

$1/planet-dump-ng --generator "planet-dump-ng test X.Y.Z" --changesets changesets.osm.bz2 --dump-file $1/test/bad-character.dmp
