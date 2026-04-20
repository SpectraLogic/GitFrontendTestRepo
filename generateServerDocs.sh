#!/bin/sh
#
# Will generate DS3 Server Documentation
#
# Set GRADLE_SCRATCH to a valid local directory if running on an NFS mounted
# tree to avoid lock failures in the user gradle home and project cache dirs

set -e

DIR="$(dirname $0)"
GRADLE="${DIR}/gradlew"
OPTS=""
if [ -n "$GRADLE_SCRATCH" ]; then
    OPTS="-g ${GRADLE_SCRATCH} --project-cache-dir ${GRADLE_SCRATCH}/cache"
    echo "Using additional gradle options '${OPTS}'"
    echo "Creating gradle scratch at ${GRADLE_SCRATCH}"
    mkdir -p ${GRADLE_SCRATCH}/cache
fi

echo ""
echo "Creating /tmp/frontend/ directory and setting Java tmp dir"

mkdir -p /tmp/frontend
export JDK_JAVA_OPTIONS='-Djava.io.tmpdir=/tmp/frontend'

echo ""
echo "Checking out from perforce:"
echo "  server/src/test/java/com/spectralogic/s3/server/mock/MockHttpRequestDriver.java"
echo "  server/src/main/resources/requesthandlerresponses.props"
echo "                            request-handlers.xml"
echo "                            request-handlers-full.xml"
{ # this is my bash try block
    p4 edit server/src/test/java/com/spectralogic/s3/server/mock/MockHttpRequestDriver.java
} || { # this is catch block
    echo ""
    echo ""
    echo "* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *"
    echo "* Failed to check out files from Perforce.  See possible solutions below. *"
    echo "* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *"
    if [ `uname -o` = "Cygwin" ]
    then
        echo ""
        echo ""
        echo "When running this script under Cygwin, be sure to include the following in your .bashrc:"
        echo ""
        echo "function p4() {"
        echo "  export PWD=\`cygpath -wa .\`"
        echo "  /cygdrive/c/Program\ Files/Perforce/p4.exe \$@"
        echo "}"
    fi
    
    echo ""
    echo ""
    echo "Be sure you are logged in via command: p4 login -a"
    echo ""
    echo ""
    echo "The correct p4 configuration must be determined automatically for this script to work.  You are strongly encouraged to achieve this using a .p4config file (with a P4CONFIG environment variable).  For more information, see http://www.perforce.com/perforce/doc.current/manuals/p4guide/chapter.configuration.html#configuration.settings.configfiles"
    echo ""
    exit 1
}

p4 edit server/src/main/resources/requesthandlerresponses.props
p4 edit server/src/main/resources/request-handlers.xml
p4 edit server/src/main/resources/request-handlers-full.xml
p4 edit server/src/main/resources/request-handlers-contract.xml

#2) Modify the line in MockHttpRequestDriver to generate docs at the bottom of the file
echo ""
echo "Modifying MockHttpRequestDriver to generate example responses..."
sedfile=server/src/test/java/com/spectralogic/s3/server/mock/MockHttpRequestDriver.java
sed -e 's/EXAMPLE_RESPONSES = null/EXAMPLE_RESPONSES = new SortedProperties()/g' "${sedfile}" > "${sedfile}.new" && mv "${sedfile}.new" "${sedfile}"

#3) Run testServer.sh (or equiv. with Gradle if temp dir needs to be specified)
echo ""
echo "Running Server tests to update requesthandlerresponses.props..."
set +e
./testServer.sh
echo "Note: The test run that just completed may have failed due to documentation being out of date, which is not a problem if it did."
set -e

#4) Copy requesthandlerresponses.props from temp dir to src dir
[ -n "$BUILD_DEBUG" ] && OPTS="${OPTS}${OPTS+\ }--stacktrace --debug --offline "
$GRADLE $OPTS copyRequestHandlerResponses

#5) Run GetAllRequestHandlers_Test only, to re-generate XML files
echo ""
echo "Regenerating Request Handler XML files..."
echo "Note: Test run may fail due to documentation being out of date, but it will still generate the desired result."
# explicitly allow expected failure
set +e
$GRADLE $OPTS -Dtest.single=GetRequestHandlersRequestHandler_Test server:test
echo "Note: The test run that just completed may have failed due to documentation being out of date, which is not a problem if it did."
set -e
echo ""

#6) Copy request-handlers.xml and request-handlers-full.xml from temp dir to src dir
$GRADLE $OPTS copyRequestHandlerXml

#7) p4 revert MockHttpRequestDriver
echo ""
echo "Reverting MockHttpRequestDriver..."
p4 revert server/src/test/java/com/spectralogic/s3/server/mock/MockHttpRequestDriver.java

echo ""
echo "The following files have been updated:"
echo "  server/src/main/resources/requesthandlerresponses.props"
echo "                            request-handlers.xml"
echo "                            request-handlers-full.xml"
echo ""
echo "They are in your P4 default changelist, and SHOULD BE INSPECTED BEFORE SUBMITTING."
echo ""
echo "BUILD SUCCESSFUL"
