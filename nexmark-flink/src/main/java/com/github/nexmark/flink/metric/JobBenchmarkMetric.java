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

package com.github.nexmark.flink.metric;

import java.util.Objects;

import static com.github.nexmark.flink.metric.BenchmarkMetric.NUMBER_FORMAT;
import static com.github.nexmark.flink.metric.BenchmarkMetric.formatLongValue;

public class JobBenchmarkMetric {
	private final double tps;
	private final double cpu;
	private final long eventsNum;
	private final long timeMills;

	public JobBenchmarkMetric(double tps, double cpu) {
		this(tps, cpu, 0, 0);
	}

	public JobBenchmarkMetric(double tps, double cpu, long eventsNum, long timeMills) {
		this.tps = tps;
		this.eventsNum = eventsNum;
		this.cpu = cpu;
		this.timeMills = timeMills;
	}

	public String getPrettyTps() {
		return formatLongValue((long) tps);
	}

	public long getEventsNum() {
		return eventsNum;
	}

	public double getTimeSeconds() {
		return timeMills / 1000D;
	}

	public String getPrettyCpu() {
		return NUMBER_FORMAT.format(cpu);
	}

	public double getCpu() {
		return cpu;
	}

	public String getPrettyTpsPerCore() {
		return formatLongValue(getTpsPerCore());
	}

	public long getTpsPerCore() {
		return (long) (tps / cpu);
	}

	public double getCoresMultiplyTimeSeconds() {
		return cpu * getTimeSeconds();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JobBenchmarkMetric that = (JobBenchmarkMetric) o;
		return Double.compare(that.tps, tps) == 0 &&
				eventsNum == that.eventsNum &&
				Double.compare(that.cpu, cpu) == 0 &&
				timeMills == that.timeMills;
	}

	@Override
	public int hashCode() {
		return Objects.hash(tps, eventsNum, cpu, timeMills);
	}

	@Override
	public String toString() {
		return "BenchmarkMetric{" +
				"tps=" + tps +
				", eventsNum=" + eventsNum +
				", cpu=" + cpu +
				", timeMills=" + timeMills +
				'}';
	}
}
