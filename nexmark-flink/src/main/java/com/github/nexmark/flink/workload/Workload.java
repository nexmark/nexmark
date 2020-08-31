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

import com.github.nexmark.flink.metric.BenchmarkMetric;

import java.util.Objects;

public class Workload {

	private final long tps;
	private final int personProportion;
	private final int auctionProportion;
	private final int bidProportion;

	public Workload(long tps, int personProportion, int auctionProportion, int bidProportion) {
		this.tps = tps;
		this.personProportion = personProportion;
		this.auctionProportion = auctionProportion;
		this.bidProportion = bidProportion;
	}

	public long getTps() {
		return tps;
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

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Workload workload = (Workload) o;
		return tps == workload.tps &&
			personProportion == workload.personProportion &&
			auctionProportion == workload.auctionProportion &&
			bidProportion == workload.bidProportion;
	}

	@Override
	public int hashCode() {
		return Objects.hash(tps, personProportion, auctionProportion, bidProportion);
	}

	public String getSummaryString() {
		return String.format(
			"[tps=%s, percentage=bid:%s,auction:%s,person:%s]",
			BenchmarkMetric.formatLongValue(tps),
			bidProportion,
			auctionProportion,
			personProportion);
	}

	@Override
	public String toString() {
		return "Workload{" +
			"tps=" + tps +
			", personProportion=" + personProportion +
			", auctionProportion=" + auctionProportion +
			", bidProportion=" + bidProportion +
			'}';
	}
}
