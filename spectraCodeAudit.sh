#!/bin/sh
#
# Will run tests for the DS3 server.


javac -cp "lib/*;util/build/classes/test" -d "util/build/classes/test" util/src/test/java/com/spectralogic/util/manualrun/ManualRunUtil.java

javac -cp "lib/*;util/build/classes/test" -d "util/build/classes/test" util/src/test/java/com/spectralogic/util/manualrun/SpectraCodeAudit.java

java -d64 -server -cp "lib/*;util/build/classes/test" com.spectralogic.util.manualrun.SpectraCodeAudit
