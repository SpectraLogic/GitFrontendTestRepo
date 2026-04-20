#!/bin/bash
set -e



echo "Starting Simulator Application..."
exec java -Dlog4j.configuration=file:/app/log4j.xml -Dlog4j2.disable.jmx=true -cp /usr/local/bluestorm/frontend/simulator/bin/simulator-0.0.4-SNAPSHOT.jar:/usr/local/bluestorm/frontend/dataplanner/bin/lib/* com.spectralogic.s3.simulator.Simulator
