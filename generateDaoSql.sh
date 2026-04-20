#!/bin/sh
#
# Will compile and run common tests, then compile the dao SQL code.

DIR="$(dirname $0)"
GRADLE="${DIR}/gradlew"
${GRADLE} --offline compileSql
