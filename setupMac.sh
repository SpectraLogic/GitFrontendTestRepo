#!/bin/sh
# NB: This script should consist of idempotent actions, so it can be rerun.
set -x

brew install postgresql gradle

# Configure postgresql to run and start it.
pg_plist=/usr/local/opt/postgresql/*.plist
chmod 600 $pg_plist
ln -sfv $pg_plist ~/Library/LaunchAgents
launchctl load ~/Library/LaunchAgents/homebrew.mxcl.postgresql.plist
exit 0

# Create the databases used.
createuser -sdr Administrator
createuser -sdr postgres

# Tell launchd about the postgresql binaries
LAUNCHCTL_PATH=$(launchctl getenv PATH)
case "$LAUNCHCTL_PATH" in
*/usr/local/bin*) ;;
*)
	launchctl setenv PATH ${LAUNCHCTL_PATH}:/usr/local/bin
	osascript -e 'tell app "Dock" to quit'
	;;
esac
