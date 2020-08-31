#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# setup side input files
USAGE="Usage: side_input_gen.sh"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

java -cp "$NEXMARK_HOME/lib/*:$FLINK_HOME/lib/*" com.github.nexmark.flink.generator.SideInputGenerator --num 10000 --path "$FLINK_HOME/data/side_input.txt"