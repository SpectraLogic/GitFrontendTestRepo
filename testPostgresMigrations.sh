#!/bin/bash


# This script is coded under the assumtion that it's only used as a development
# tool, only on development boxes. As such, some parts of it will likely not
# play nice in test or production build environments.


# Passing in a Ruby version value only makes sense in RVM-enabled envs.
unset USE_RUBY_VERSION
[ -n "$1" ] && USE_RUBY_VERSION="$1"


COMP_DBS="./compare_databases.rb"

DAO_SQL="common/build/libs/dao.sql"

#SQL_GEN="./generatePostgresSql.sh"
SQL_GEN="./generateDaoSql.sh"

MIN_RUBY_VERSION="2.1.9"

DB_RAKEFILE="sql/Rakefile"

EXPECTED_PSQL="/usr/local/bin/psql"

SL_DB_ROLE="Administrator"

DEFAULT_DB_ROLE="postgres"

FRONTEND_DEV_CONFIG="'/redline-main/product/Documents/Bluestorm Front End Developer Readme.docx'"


# Note on 'rake' gem: Not all Ruby env managers/install approaches automatically
# install 'rake', and although this script uses RVM if it's available (and RVM
# does include 'rake' as part of a Ruby install), we do not assume RVM is
# available, nor that the Ruby install to this machine included 'rake'.
GEM_DEPENDENCIES="rake"
GEM_DEPENDENCIES="$GEM_DEPENDENCIES pg"
GEM_DEPENDENCIES="$GEM_DEPENDENCIES multi_json"
GEM_DEPENDENCIES="$GEM_DEPENDENCIES i18n"
GEM_DEPENDENCIES="$GEM_DEPENDENCIES activesupport"
GEM_DEPENDENCIES="$GEM_DEPENDENCIES builder"
GEM_DEPENDENCIES="$GEM_DEPENDENCIES activemodel"
GEM_DEPENDENCIES="$GEM_DEPENDENCIES tzinfo"
GEM_DEPENDENCIES="$GEM_DEPENDENCIES arel"
GEM_DEPENDENCIES="$GEM_DEPENDENCIES activerecord"
GEM_DEPENDENCIES="$GEM_DEPENDENCIES open4"


if ! [ -f "$COMP_DBS" ] || ! [ -r "$COMP_DBS" ]
then
    echo
    echo "Either can't find Ruby script '$COMP_DBS' or it's not readable. Exiting."
    echo "Expected '$COMP_DBS' to be in dir '$PWD'."
    exit 1
fi


if ! [ -f "$SQL_GEN" ] || ! [ -x "$SQL_GEN" ]
then
    echo
    echo "Either can't find script '$SQL_GEN' or it's not executable. Exiting."
    echo "Expected to find '$SQL_GEN' below dir '$PWD'."
    exit 1
fi

echo
echo "Launching $SQL_GEN"
echo
if ! $SQL_GEN
then
    echo
    echo "'$SQL_GEN' failed. Exiting."
    echo "Try running './testAll.sh' before '${0##*/}'."
    exit 1
fi


if ! [ -f "$DAO_SQL" ]
then
    echo
    echo "Requied file '$DAO_SQL' does not exist. Exiting."
    echo "Expected to find '$DAO_SQL' below dir '$PWD'."
    echo "'$SQL_GEN' generates file '$DAO_SQL'."
    exit 1
fi

if ! [ -r "$DAO_SQL" ] || ! chmod u+r "$DAO_SQL"
then
    echo
    echo "Requied file '$DAO_SQL' is not readable. Exiting."
    exit 1
fi


if ! [ -f "$DB_RAKEFILE" ]
then
    echo
    echo "Requied file '$DB_RAKEFILE' does not exist. Exiting."
    echo "Expected to find '$DB_RAKEFILE' below dir '$PWD'."
    exit 1
fi

if ! [ -r "$DB_RAKEFILE" ] || ! chmod u+r "$DAO_SQL"
then
    echo
    echo "Requied file '$DB_RAKEFILE' is not readable. Exiting."
    exit 1
fi


echo
if ! [ -f "$HOME/.rvm/scripts/rvm" ]
then
    if [ -n "$USE_RUBY_VERSION" ]
    then
        echo "Passing in a Ruby version only makes sense in RVM-enabled envs."
        echo
    fi
    echo "RVM not found. If a Ruby env exists on this host, but it does not"
    echo "successfully run '$COMP_DBS', see ${FRONTEND_DEV_CONFIG}"
    echo "for RVM-based Ruby install instructions."

else
    echo "Configuring Ruby env with RVM."
    source "$HOME/.rvm/scripts/rvm"

    if [ -z "$USE_RUBY_VERSION" ]
    then

        RUBY_VERSIONS=$( rvm list |
                         grep -Eo 'ruby-.* ' |
                         sed -r 's| .*$||' |
                         cut -d '-' -f2 )
        unset RUBY_VNS

        for V in $RUBY_VERSIONS
        do
            RUBY_VNS="$RUBY_VNS $V"
        done

        # Assumptions about the format of Ruby version numbers are likely
        # fragile, but they do the trick for now. It'll be obvious when they no
        # longer do, and they can be addressed at that time.
        MAX_RUBY_VERSION=$( echo "$RUBY_VNS" |
                            tr ' ' '\n' |
                            sort -br |
                            tr '\n' ' ' |
                            cut -d ' ' -f1 )
        echo
        echo "Setting Ruby to latest of installed versions: $(
                                           echo $RUBY_VERSIONS | tr '\n' ' ' )"
        USE_RUBY_VERSION="$MAX_RUBY_VERSION"

        unset RUBY_VERSIONS RUBY_VNS
    fi

    if ! rvm use $USE_RUBY_VERSION
    then
        echo
        echo "RVM failed to set Ruby version. Exiting."
        exit 1
    fi
fi


# We test for Ruby independent of, and after, attempting to use RVM, so that
# non-RVM hosts that have a functioning Ruby env can also use this script.
if ! which ruby >/dev/null
then
    echo
    echo "Can't find 'ruby'. '${0##*/}' is mostly a Ruby script wrapper. Exiting."
    echo "See the $FRONTEND_DEV_CONFIG for Ruby install instructions."
    exit 1
fi


# Assumptions about the format of Ruby version numbers are likely fragile, but
# they do the trick for now. It'll be obvious when they no longer do, and they
# can be addressed at that time.
RV=$( ruby -v | cut -d ' ' -f2 | sed -r 's|[a-zA-Z].*$||' | tr -d '.' )

if [ "$RV" -lt "$( echo $MIN_RUBY_VERSION | tr -d '.' )" ]
then
    RV=$( ruby -v | cut -d ' ' -f2 | sed -r 's|[a-zA-Z].*$||' )
    echo
    echo "Min required Ruby: $MIN_RUBY_VERSION  Actual: ${RV}  Exiting."
    exit 1
fi

echo


echo "Verifying Ruby gem dependencies are installed."

for G in $GEM_DEPENDENCIES
do
    if ! gem list "$G" -i --local &>/dev/null
    then
        echo
        echo "Required Ruby gem '$G' is not available. Exiting."
        echo "See $FRONTEND_DEV_CONFIG for Ruby install instructions."
        exit 1
    fi
done


echo
echo "Confirming 'psql' is available and Postgres DB user role '$SL_DB_ROLE' exists."

ACTUAL_PSQL=$( which psql )
if [ -z "$ACTUAL_PSQL" ]
then
    echo
    echo "Can't find required 'psql'. Exiting."
    echo "See $FRONTEND_DEV_CONFIG for Postgres install instructions."
    exit 1
fi


if [ "$EXPECTED_PSQL" != "$ACTUAL_PSQL" ]
then
    echo
    echo "'$COMP_DBS' requires: '$EXPECTED_PSQL'; actual: '$ACTUAL_PSQL'. Exiting."
    echo "See the Ruby section of $FRONTEND_DEV_CONFIG"
    echo "for how to set a soft link to the actual location in the dir that"
    echo "'$COMP_DBS' expects."
    exit 1
fi


PATH_SEPARATOR=":"

if uname | grep -E "(CYGWIN|Windows|WINDOWS)+" &>/dev/null
then
    PATH_SEPARATOR=";"

    echo
    echo "Setting PGHOST to 'localhost' for Postgres client C library use in Cygwin env."

    # Without this MANY direct 'psql' calls from within Ruby scripts,
    # and numerous direct Postgres DB accesses from the 'pg' gem,
    # would need updated to not by default try to use a Unix socket to
    # connect to the local Postgres DB (at least when running in Cygwin).
    export PGHOST=localhost
fi


if ! psql -U $DEFAULT_DB_ROLE \
          -c "SELECT rolname FROM pg_roles WHERE rolname='$SL_DB_ROLE'" |
     grep -o "$SL_DB_ROLE" &>/dev/null
then
    echo
    echo \
    "Required DB role '$SL_DB_ROLE' does not seem to exist. Trying to create it."
    if ! psql -U $DEFAULT_DB_ROLE \
              -c "CREATE USER $SL_DB_ROLE WITH SUPERUSER;"
    then
        echo "Failed to created DB role '$SL_DB_ROLE'. Exiting."
        exit 1
    fi
    echo "Role '$SL_DB_ROLE' created."
fi


unset RUBYLIB

# Make SL Ruby gems available to COMP_DBS, which needs at least one of them.
for D in $( find ${PWD%/*/*}/rubygems/ -maxdepth 2 -type d -name lib )
do
    RUBYLIB="$RUBYLIB$D$PATH_SEPARATOR"
done


echo
echo "PATH=$PATH"
export PATH
echo
echo "RUBYLIB=$RUBYLIB"
export RUBYLIB


echo
echo "Launching $COMP_DBS"
echo
ruby "$COMP_DBS" "$DAO_SQL" "$DB_RAKEFILE"

