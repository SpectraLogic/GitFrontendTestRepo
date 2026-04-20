#!/bin/sh

DIR="$(dirname $0)"
GRADLE="${DIR}/gradlew"

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

$GRADLE $OPTS $NO_DAEMON --rerun-tasks clean target:test --fail-fast