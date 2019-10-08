#!/usr/bin/env bash
echo "compiling project presto 320"
cd ../../
mvn clean install -DskipTests
cd presto-server/target
tar -zxf presto-server-320.tar.gz
cp -r ../../deploy/PRESTO/package presto-server-320
tar -czf presto-server-320.tar.gz presto-server-320