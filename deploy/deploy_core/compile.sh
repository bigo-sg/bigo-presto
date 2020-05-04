#!/usr/bin/env bash
echo "compiling project presto 332"
cd ../../
mvn clean install -DskipTests
cd presto-server/target
tar -zxf presto-server-332.tar.gz
cp -r ../../deploy/PRESTO/package presto-server-332
tar -czf presto-server-332.tar.gz presto-server-332
