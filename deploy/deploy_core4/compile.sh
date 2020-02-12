#!/usr/bin/env bash
echo "compiling project presto 324"
cd ../../
mvn clean install -DskipTests
cd presto-server/target
tar -zxf presto-server-324.tar.gz
cp -r ../../deploy/PRESTO/package presto-server-324
tar -czf presto-server-324.tar.gz presto-server-324