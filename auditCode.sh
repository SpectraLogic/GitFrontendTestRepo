#!/bin/sh
#
# Will run the Spectra code formatting audit tool

mkdir -p ./devtool/build/classes/main

rm -f ./devtool/build/classes/main/com/spectralogic/devtool/SpectraAuditCode*        

javac -cp "lib/*;devtool/build/classes/main" -d "devtool/build/classes/main" devtool/src/main/java/com/spectralogic/devtool/SpectraAuditCode.java

java -d64 -server -cp "lib/*;devtool/build/classes/main;devtool/src/main/resources" com.spectralogic.devtool.SpectraAuditCode
