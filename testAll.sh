#!/bin/sh

# Check if the SKIP_PUBLIC_CLOUD_TESTS environment variable is not set.
# If it's not set, the script will exit.
if [ -z "${SKIP_PUBLIC_CLOUD_TESTS}" ]; then
   echo "The SKIP_PUBLIC_CLOUD_TESTS environment variable is not set. Starting docker containers for public cloud tests."
  bash start_docker_testAll.sh
fi

# Uses Gradle to clean build Frontend and run all its components' JUnit tests.

DIR="$(dirname $0)"
GRADLE="${DIR}/gradlew"

I=1
if [ -n "$1" ]
then
    if ! [ "-1" -lt "$1" ] 2>/dev/null
    then
        echo
        echo "'$1' is not a legitimate number of test run iterations. Exiting."
        exit 1
    fi

    I=$1
    [ "0" = "$I" ] && I=1
fi


# While knowledge of these vars' values are obvious on dev boxes, this is not
# necessarily the case in offical Spectra build envs, at least not for Frontend
# focused developers, who might need to diagnose and fix errors in those evns:
echo
echo "$0: PWD=$PWD"
echo "$0: HOME=$HOME"
echo "$0: USER=$USER"
echo "$0: SHELL=$SHELL"


# The AspectJ compiler seems to have a char encoding bug that's only exposed
# in some build host envs/configs. Eliminate it as a possibility in any env:
export LANG="en_US.UTF-8"
echo "$0: LANG=$LANG"

NO_DAEMON=""

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


[ -n "$BUILD_DEBUG" ] && OPTS="--stacktrace --debug"

$GRADLE -version             # Log basic version info of conerstone build tools.

i=0
while [ $I -gt $i ]
do
    # Gradle seems to have a few different caching contexts, some (one) of which
    # are used across distinct Gradle invocations. '--rerun-tasks' seems to
    # clear out (one of?) the across-invocation cache(s), which is certainly
    # what's needed in every production build.
    
    # Add "-Duse.aspectj" after $GRADLE to enable thread leak detection during test.

   # if ! $GRADLE $OPTS $NO_DAEMON --offline --rerun-tasks clean test --stacktrace
   if ! $GRADLE $OPTS $NO_DAEMON --rerun-tasks clean test --fail-fast --scan
    then
        echo
        echo "Successful full test runs before failure: $i"
        exit 1
    fi

    i=`expr $i + 1`
    echo
    echo "Successful full test runs: $i"
    echo
done

#Commenting this part out. docker-compose is not available in test env
# Run the DB migration check using docker
#docker-compose -f dbmigrationtest/docker-compose-testdb.yml run --rm databasetest bundle exec ruby compare_databases.rb dao.sql Rakefile > /dev/null

# Capture the exit code of the last command
#exit_code=$?

# Check the exit code
#if [ $exit_code -eq 0 ]; then
#  echo "DB migration tests ran successfully."
#else
  #echo "DB migration tests with exit code: $exit_code"
  # You can also exit the script with the same code
  #exit $exit_code
#fi
