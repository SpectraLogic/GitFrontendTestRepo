#!/bin/sh -x

add-apt-repository "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -sc)-pgdg main"
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y openjdk-8-jdk postgresql-9.6
sed -i -e 's/peer/trust/' /etc/postgresql/9.6/main/pg_hba.conf
service postgresql restart
sudo -u postgres createuser Administrator --superuser --no-password
