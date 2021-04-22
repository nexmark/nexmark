/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.nexmark.flink.workload;

import org.apache.flink.util.Preconditions;

import com.github.nexmark.flink.FlinkNexmarkOptions;
import com.github.nexmark.flink.metric.BenchmarkMetric;

import java.time.Duration;
import java.util.Objects;

public class Workload {

	private final long tps;
	private final long eventsNum;
	private final int personProportion;
	private final int auctionProportion;
	private final int bidProportion;

	public Workload(long tps, long eventsNum, int personProportion, int auctionProportion, int bidProportion) {
		this.tps = tps;
		this.eventsNum = eventsNum;
		this.personProportion = personProportion;
		this.auctionProportion = auctionProportion;
		this.bidProportion = bidProportion;
	}

	public long getTps() {
		return tps;
	}

	public long getEventsNum() {
		return eventsNum;
	}

	public int getPersonProportion() {
		return personProportion;
	}

	public int getAuctionProportion() {
		return auctionProportion;
	}

	public int getBidProportion() {
		return bidProportion;
	}

	public void validateWorkload(Duration monitorDuration) {
		boolean unboundedMonitor = monitorDuration.toMillis() == Long.MAX_VALUE;
		if (getEventsNum() == 0) {
			// TPS mode
			Preconditions.checkArgument(
					!unboundedMonitor,
					"You should configure '%s' in the TPS mode." +
							" Otherwise, the job will never end.",
					FlinkNexmarkOptions.METRIC_MONITOR_DURATION.key());
		} else {
			// EventsNum mode
			Preconditions.checkArgument(
					unboundedMonitor,
					"The configuration of '%s' is not supported" +
							" in the events number mode.",
					FlinkNexmarkOptions.METRIC_MONITOR_DURATION.key());
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Workload workload = (Workload) o;
		return tps == workload.tps &&
			eventsNum == workload.eventsNum &&
			personProportion == workload.personProportion &&
			auctionProportion == workload.auctionProportion &&
			bidProportion == workload.bidProportion;
	}

	@Override
	public int hashCode() {
		return Objects.hash(tps, eventsNum, personProportion, auctionProportion, bidProportion);
	}

	public String getSummaryString() {
		return String.format(
			"[tps=%s, eventsNum=%s, percentage=bid:%s,auction:%s,person:%s]",
			BenchmarkMetric.formatLongValue(tps),
			BenchmarkMetric.formatLongValue(eventsNum),
			bidProportion,
			auctionProportion,
			personProportion);
	}

	@Override
	public String toString() {
		return "Workload{" +
			"tps=" + tps +
			", eventsNum=" + eventsNum +
			", personProportion=" + personProportion +
			", auctionProportion=" + auctionProportion +
			", bidProportion=" + bidProportion +
			'}';
	}
}
