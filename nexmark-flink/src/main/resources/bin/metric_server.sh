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

USAGE="Usage: metric-server.sh (start|stop)"

if [ $# -lt 1 ]; then
  echo $USAGE
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

STARTSTOP=$1

case $STARTSTOP in
    (start)
        log=$NEXMARK_LOG_DIR/metric-server.log
        log_setting=(-Dlog.file="$log" -Dlog4j.configuration=file:"$NEXMARK_CONF_DIR"/log4j.properties -Dlog4j.configurationFile=file:"$NEXMARK_CONF_DIR"/log4j.properties)
        java "${log_setting[@]}" -cp "$NEXMARK_HOME/lib/*:$FLINK_HOME/lib/*" com.github.nexmark.flink.metric.cpu.CpuMetricReceiver &
    ;;

    (stop)
        PID="$(jps | grep CpuMetricReceiver | awk '{print $1}')"
        kill -9 $PID
        echo "$PID CpuMetricReceiver has been killed."
    ;;
esac



