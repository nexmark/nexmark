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

import java.text.NumberFormat;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public class BenchmarkMetric {
	private final double tps;
	private final double cpu;
	private final boolean preciseMode;

	public BenchmarkMetric(double tps, double cpu) {
		this(tps, cpu, false);
	}

	public BenchmarkMetric(double tps, double cpu, boolean preciseMode) {
		this.tps = tps;
		this.cpu = cpu;
		this.preciseMode = preciseMode;
	}

	public double getTps() {
		return tps;
	}

	public String getPrettyTps() {
		if (preciseMode) {
			return tps + "";
		} else {
			return formatLongValue((long) tps);
		}
	}

	public double getCpu() {
		return cpu;
	}

	public String getPrettyCpu() {
		return NUMBER_FORMAT.format(cpu);
	}

	public String getFormattedTpsPerCore() {
		long result = (long) (tps / cpu);
		if (preciseMode) {
			return result + "";
		} else {
			return formatLongValue(result);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		BenchmarkMetric that = (BenchmarkMetric) o;
		return Double.compare(that.tps, tps) == 0 &&
			Double.compare(that.cpu, cpu) == 0;
	}

	@Override
	public int hashCode() {
		return Objects.hash(tps, cpu);
	}

	@Override
	public String toString() {
		return "BenchmarkMetric{" +
			"tps=" + tps +
			", cpu=" + cpu +
			'}';
	}


	// -------------------------------------------------------------------------------------------
	// Pretty Utilities
	// -------------------------------------------------------------------------------------------
	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
	private static final NavigableMap<Long, String> SUFFIXES = new TreeMap<>();
	static {
		SUFFIXES.put(1_000L, "K");
		SUFFIXES.put(1_000_000L, "M");
		SUFFIXES.put(1_000_000_000L, "G");
		SUFFIXES.put(1_000_000_000_000L, "T");
		SUFFIXES.put(1_000_000_000_000_000L, "P");
		SUFFIXES.put(1_000_000_000_000_000_000L, "E");
		NUMBER_FORMAT.setMaximumFractionDigits(2);
	}

	public static String formatLongValue(long value) {
		//Long.MIN_VALUE == -Long.MIN_VALUE so we need an adjustment here
		if (value == Long.MIN_VALUE) return formatLongValue(Long.MIN_VALUE + 1);
		if (value < 0) return "-" + formatLongValue(-value);
		if (value < 1000) return Long.toString(value); //deal with easy case

		Map.Entry<Long, String> e = SUFFIXES.floorEntry(value);
		Long divideBy = e.getKey();
		String suffix = e.getValue();

		long truncated = value / (divideBy / 10); //the number part of the output times 10
		boolean hasDecimal = truncated < 100 && (truncated / 10d) != (truncated / 10);
		return hasDecimal ? (truncated / 10d) + " " + suffix : (truncated / 10) + " " + suffix;
	}
}
