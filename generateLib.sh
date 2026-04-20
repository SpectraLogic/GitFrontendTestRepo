#!/bin/sh
#
# Will re-generate the lib directory with all third-party dependencies.  After running this script, be sure
# to p4 reconcile the lib directory and check in the reconciled results.

DIR="$(dirname $0)"
echo "Creating /tmp/frontend/ directory and setting Java tmp dir"
GRADLE="${DIR}/gradlew"
mkdir -p /tmp/frontend
export JDK_JAVA_OPTIONS='-Djava.io.tmpdir=/tmp/frontend'

rm -rf lib-temp
mkdir lib-temp
mv lib/fast-md5-* lib-temp/
mv lib/ds3-sdk-* lib-temp/

rm -rf lib
mkdir lib

echo "Generating lib directory (fast-md5)..."
mv lib-temp/* lib/
rmdir lib-temp

echo "Generating lib directory (all other JARs)..."
${GRADLE} clean copyToLib

echo ""
echo ""
echo "Removing JARs that aren't actually 3rd party lib dependencies:"
ls lib | grep SNAPSHOT.jar
rm lib/*SNAPSHOT.jar