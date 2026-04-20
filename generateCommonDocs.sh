#!/bin/sh
#
# Will generate DS3 Notification Documentation
#
# Set GRADLE_SCRATCH to a valid local directory if running on an NFS mounted
# tree to avoid lock failures in the user gradle home and project cache dirs
#
#1) Check out out of Perforce
#    a) common/src/test/java/com/spectralogic/s3/common/platform/notification/generator/NotificationPayloadTracker.java
#    b) common/src/main/resources/notification-payloads.xml
#2) Modify NotificationPayloadTracker to generate docs
#3) Run tests for common component
#4) Copy notification-payloads.xml from temp dir to src dir
#5) p4 revert NotificationPayloadTracker

OPTS=""
if [ -n "$GRADLE_SCRATCH" ]; then
    OPTS="-g ${GRADLE_SCRATCH} --project-cache-dir ${GRADLE_SCRATCH}/cache"
    echo "Using additional gradle options '${OPTS}'"
    echo "Creating gradle scratch at ${GRADLE_SCRATCH}"
    mkdir -p ${GRADLE_SCRATCH}/cache
fi

set -e

DIR="$(dirname $0)"
GRADLE="${DIR}/gradlew"

echo ""
echo "Creating /tmp/frontend/ directory and setting Java tmp dir"

mkdir -p /tmp/frontend
export JDK_JAVA_OPTIONS='-Djava.io.tmpdir=/tmp/frontend'

echo ""
echo "Checking out from perforce:"
echo "  common/src/test/java/com/spectralogic/s3/common/platform/notification/generator/NotificationPayloadTracker.java"
echo "  common/src/main/resources/notification-payloads.xml"
{ # this is my bash try block
    p4 edit common/src/test/java/com/spectralogic/s3/common/platform/notification/generator/NotificationPayloadTracker.java
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
        echo "  export PWD=`cygpath -wa .`"
        echo "  /cygdrive/c/Program\ Files/Perforce/p4.exe $@"
        echo "}"
    fi

    echo ""
    echo ""
    echo "Be sure you are logged in via command: p4 login -a"
    echo ""
    echo ""
    echo "The correct p4 configuration must be determined automatically for this script to work.You are strongly encouraged to achieve this using a .p4config file.  For more information, see http://www.perforce.com/perforce/doc.current/manuals/p4guide/chapter.configuration.html#configuration.settings.configfiles"
    echo ""
    exit 1
}

p4 edit common/src/main/resources/notification-payloads.xml

#2) Modify NotificationPayloadTracker to generate docs
echo ""
echo "Modifying NotificationPayloadTracker to generate docs..."
sedfile=common/src/test/java/com/spectralogic/s3/common/platform/notification/generator/NotificationPayloadTracker.java
sed -e 's/\/\/        persistPayloads();/        persistPayloads();/g' "${sedfile}" > "${sedfile}.new" && mv "${sedfile}.new" "${sedfile}"

#3) Run tests for common component
echo ""
echo "Running common tests..."
echo "Note: Test run may fail due to documentation being out of date, but it will still generate the desired result."
# explicitly allow expected failure
set +e
[ -n "$BUILD_DEBUG" ] && OPTS="${OPTS}${OPTS+\ }--stacktrace --debug --offline "
$GRADLE $OPTS common:test
echo "Note: The test run that just completed may have failed due to documentation being out of date, which is not a problem if it did."
set -e
echo ""

#4) Copy notification-payloads.xml from temp dir to src dir
$GRADLE $OPTS copyNotificationPayloadsXml

#7) p4 revert NotificationPayloadTracker
echo ""
echo "Reverting NotificationPayloadTracker..."
p4 revert common/src/test/java/com/spectralogic/s3/common/platform/notification/generator/NotificationPayloadTracker.java

echo ""
echo "common/src/main/resources/notification-payloads.xml has been updated."
echo ""
echo "It is in your P4 default changelist, and SHOULD BE INSPECTED BEFORE SUBMITTING."
echo ""
echo "BUILD SUCCESSFUL"
