#!/bin/bash

set -e

TMPDIR=`mktemp -d 2>/dev/null || mktemp -d -t 'planet-dump-ng-tmp'`
SRCDIR=$(pwd)

function cleanup {
	 cd $SRCDIR
	 rm -rf $TMPDIR
}

trap cleanup EXIT

test_case=$1
if [ ! -d "$test_case" ]; then
	 echo "Test case directory '$test_case' does not exist." 1>&2
	 exit 1
fi
if [ ! -x "$test_case/cmd.sh" ]; then
	 echo "Test case does not contain command to run." 1>&2
	 exit 1
fi

# switch to the temporary directory
cd $TMPDIR

# run the test case
$SRCDIR/$test_case/cmd.sh $SRCDIR
if [ $? -ne 0 ]; then
	 echo "Failed to run test case." 2>&1
	 exit 1
fi

# compare the outputs
for name in *.bz2; do
	 if [ -f "$name" ]; then
		  bzcmp "$name" "$SRCDIR/$test_case/$name"
		  if [ $? -ne 0 ]; then
				echo "Output '$name' does not match '$SRCDIR/$test_case/$name'"
				exit 1
		  fi
	 fi
done
for name in *.pbf; do
	 if [ -f "$name" ]; then
		  cmp "$name" "$SRCDIR/$test_case/$name"
		  if [ $? -ne 0 ]; then
				echo "Output '$name' does not match '$SRCDIR/$test_case/$name'"
				exit 1
		  fi
	 fi
done

cd $OLDPWD
exit 0
