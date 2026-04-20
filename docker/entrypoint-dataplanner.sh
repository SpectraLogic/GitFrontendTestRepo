#!/bin/bash
set -e

echo "Waiting for Postgres to be ready..."
until PGPASSWORD=$DB_PASS psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 2
done

echo "Starting Java Application Dataplanner..."
exec java -Xmx1536m -Xms256m -Dlog4j.configuration=file:/app/log4j.xml -Dlog4j2.disable.jmx=true -cp  /usr/local/bluestorm/frontend/dataplanner/bin/DataPlanner-0.0.4-SNAPSHOT.jar:/usr/local/bluestorm/frontend/dataplanner/bin/lib/* com.spectralogic.s3.dataplanner.DataPlanner