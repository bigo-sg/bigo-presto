#!/usr/bin/env bash
echo "compiling project presto 315"
cd ../../
mvn clean install -DskipTests
cd presto-server/target
tar -zxf presto-server-315.tar.gz
cp -r ../../deploy/PRESTO/package presto-server-315
tar -czf presto-server-315.tar.gz presto-server-315