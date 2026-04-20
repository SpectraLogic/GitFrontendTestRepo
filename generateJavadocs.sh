#!/bin/sh
#
# Will generate javadocs for the project
#
# Set GRADLE_SCRATCH to a valid local directory if running on an NFS mounted
# tree to avoid lock failures in the user gradle home and project cache dirs

set -e

DIR="$(dirname $0)"
GRADLE="${DIR}/gradlew"
OPTS="--warning-mode all"
if [ -n "$GRADLE_SCRATCH" ]; then
    OPTS="-g ${GRADLE_SCRATCH} --project-cache-dir ${GRADLE_SCRATCH}/cache"
    echo "Using additional gradle options '${OPTS}'"
    echo "Creating gradle scratch at ${GRADLE_SCRATCH}"
    mkdir -p ${GRADLE_SCRATCH}/cache

fi

# In some build envs Gradle can't access $HOME/.gradle. For just these envs, we
# provide it a different location for this dir, using its GRADLE_USER_HOME env
# var. Only in these cases we set it to a dir in the dir holding this script:
if [ -z "$HOME" ] || [ ! -r "$HOME" ] || [ ! -w "$HOME" ]
then
    GRADLE_USER_HOME="${0%/*}"
    if [ -z "$GRADLE_USER_HOME" ]
    then
        echo
        echo "Unable to set GRADLE_USER_HOME. Exiting."
        exit 1
    fi
    export GRADLE_USER_HOME="$GRADLE_USER_HOME/.gradle"

    if ! [ -d "$GRADLE_USER_HOME" ] # We attempt to create it, because Gradle's
                                    # error msg is too vague when it attempts
                                    # to create it but fails.
    then
        if ! mkdir -v "$GRADLE_USER_HOME"
        then
            echo
            echo "Failed to create GRADLE_USER_HOME dir '$GRADLE_USER_HOME'. Exiting."
            exit 1
        fi
    fi
    echo "$0: GRADLE_USER_HOME=$GRADLE_USER_HOME"

    NO_DAEMON="--no-daemon" # Do not launch Gradle builds on Jenkins/production
                            # build servers using the Gradle launcher daemon.
    echo "$0: Will not launch the build with Gradle daemon: $NO_DAEMON"
fi

[ -n "$BUILD_DEBUG" ] && OPTS="${OPTS}${OPTS+\ } --stacktrace --debug"

lineSep="==========================================================================="
echo $'\n\n'
echo "Deleting existing javadocs to force regeneration and catch all new Warnings..."
echo "$lineSep"
rm -rfdv ./generateJavadoc.out.tmp
for dir in util common simulator DataPlanner server target; do
  rm -rfd $dir/build/docs/javadoc/*
  echo "Deleted $dir//build/docs/javadoc/"
done

echo $'\n\n'
echo "Building javadocs (and catching output in generateJavadoc.out.tmp)"
echo "$lineSep"
$GRADLE $OPTS  javadoc | tee generateJavadoc.out.tmp

echo $'\n\n'
echo "Checking for all warnings in all the javadocs..."
echo "$lineSep"
#grep warning generateJavadoc.out.tmp > /dev/null 2>&1
set +e
grep warning generateJavadoc.out.tmp
foundWarnings=$?
set -e
if [ $foundWarnings -eq 0 ]
  then
    echo ""
    echo ""
    echo "<<<<<<<< You have javadocs warnings, exiting.For Details see file generateJavadoc.out.tmp >>>>>>>"
    echo ""
    echo ""
    exit 1
  fi

echo $'\n\n'
echo "Packaging javadocs..."
echo "$lineSep"
rm -rf javadocs
for dir in util common simulator DataPlanner server target; do
  mkdir -p javadocs/$dir
  mv $dir/build/docs/javadoc/* javadocs/$dir
  echo "Packaged $dir."
done

cdate=`date`
sed "s/CURRENT_DATE/$cdate/g" buildconfig/javadocs-index.html > javadocs/index.html

cp common/src/main/resources/notification-payloads.xml javadocs/notification-payloads.xml
cp server/src/main/resources/request-handlers.xml javadocs/request-handlers.xml
cp server/src/main/resources/request-handlers-full.xml javadocs/request-handlers-full.xml
cp server/src/main/resources/request-handlers-contract.xml javadocs/request-handlers-contract.xml

echo $'\n\n'
echo "Javadocs created successfully under folder 'javadocs'"
echo $'\n\n'
