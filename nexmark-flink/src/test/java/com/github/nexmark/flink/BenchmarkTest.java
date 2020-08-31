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

package com.github.nexmark.flink;

import com.github.nexmark.flink.metric.BenchmarkMetric;
import org.junit.Ignore;
import org.junit.Test;

import java.util.LinkedHashMap;

@Ignore
public class BenchmarkTest {

	@Test
	public void testPrintSummary() {
		LinkedHashMap<String, BenchmarkMetric> totalMetrics = new LinkedHashMap<>();
		totalMetrics.put("q0", new BenchmarkMetric(1, 8.4));
		totalMetrics.put("q1", new BenchmarkMetric(10, 18.4));
		totalMetrics.put("q2", new BenchmarkMetric(100.0, 8.4));
		totalMetrics.put("q3", new BenchmarkMetric(1000.0, 8.4));
		totalMetrics.put("q4", new BenchmarkMetric(10_000.0, 8.4));
		totalMetrics.put("q5", new BenchmarkMetric(100_000.0, 8.4));
		totalMetrics.put("q6", new BenchmarkMetric(1_000_000.0, 8.4));
		totalMetrics.put("q7", new BenchmarkMetric(10_000_000.0, 8.23));
		totalMetrics.put("q8", new BenchmarkMetric(100_000_000.0, 8.4));
		totalMetrics.put("q9", new BenchmarkMetric(1_000_000_000.0, 8.4));
		totalMetrics.put("q10", new BenchmarkMetric(10_000_000_000.0, 8.4));
		totalMetrics.put("q11", new BenchmarkMetric(100_000_000_000.0, 8.419));
		totalMetrics.put("q12", new BenchmarkMetric(100_000_000.0, 8.4));
		totalMetrics.put("q13", new BenchmarkMetric(10_000_000.0, 8.4));
		totalMetrics.put("q14", new BenchmarkMetric(1_000_000.0, 8.4));
		Benchmark.printSummary(totalMetrics);
	}

}