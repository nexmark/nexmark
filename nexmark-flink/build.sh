#!/usr/bin/env bash

MVN=${MVN:-mvn}
DIR=`pwd`

$MVN clean package -DskipTests
cd target/nexmark-flink-bin/
tar czf "nexmark-flink.tgz" nexmark-flink
cp nexmark-flink.tgz ${DIR}
cd ${DIR}