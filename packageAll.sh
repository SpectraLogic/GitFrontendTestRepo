#!/bin/sh
#
# Package the front end code without running tests. Once complete, the latest
# code will be deployable using the deploy script.

DIR="$(dirname $0)"
GRADLE="${DIR}/gradlew"
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


# NEVER, even under threat of death should you refuse, remove 'clean' from this
# gradle call. For if you do, you'll be pushing AspectJ-injected application
# code into production.

# Gradle seems to have a few different caching contexts, some (one) of which are
# used across distinct Gradle invocations. '--rerun-tasks' seems to clear out
# (one of?) the across-invocation cache(s), which is certainly what's needed in
# every production build.

$GRADLE  $NO_DAEMON --rerun-tasks clean common:compileSql DataPlanner:distTar server:war simulator:distTar
#$GRADLE --offline $NO_DAEMON --rerun-tasks clean common:compileSql DataPlanner:distTar server:war simulator:distTar

