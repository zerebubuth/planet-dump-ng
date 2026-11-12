#!/bin/bash

$1/planet-dump-ng --generator "planet-dump-ng test X.Y.Z" --gpx gpx.gpx.bz2 --gpx-visibility identifiable trackable --dump-file $1/test/gpx-traces.dmp
