#!/bin/bash

DRIVER_VERSION=$(sed -n "/<artifactId>jdbc-build<\/artifactId>/{n;s/.*<version>\(.*\)<\/version>.*/\1/p;}" pom.xml)
if [[ $DRIVER_VERSION =~ ^.*-SNAPSHOT$ ]]; then
     echo "Version is already a SNAPSHOT: $DRIVER_VERSION"
else
     echo "Converting version $DRIVER_VERSION to SNAPSHOT"
     mvn versions:set -DnewVersion="$DRIVER_VERSION-SNAPSHOT" -DgenerateBackupPoms=false --batch-mode --no-transfer-progress -V -e -Dstyle.color=always
fi