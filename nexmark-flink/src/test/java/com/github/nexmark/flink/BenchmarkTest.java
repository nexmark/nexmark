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

import com.github.nexmark.flink.metric.JobBenchmarkMetric;
import org.junit.Ignore;
import org.junit.Test;

import java.util.LinkedHashMap;

@Ignore
public class BenchmarkTest {

	@Test
	public void testPrintSummary() {
		LinkedHashMap<String, JobBenchmarkMetric> totalMetrics = new LinkedHashMap<>();
		totalMetrics.put("q0", new JobBenchmarkMetric(1, 8.4));
		totalMetrics.put("q1", new JobBenchmarkMetric(10, 18.4));
		totalMetrics.put("q2", new JobBenchmarkMetric(100.0, 8.4));
		totalMetrics.put("q3", new JobBenchmarkMetric(1000.0, 8.4));
		totalMetrics.put("q4", new JobBenchmarkMetric(10_000.0, 8.4));
		totalMetrics.put("q5", new JobBenchmarkMetric(100_000.0, 8.4));
		totalMetrics.put("q6", new JobBenchmarkMetric(1_000_000.0, 8.4));
		totalMetrics.put("q7", new JobBenchmarkMetric(10_000_000.0, 8.23));
		totalMetrics.put("q8", new JobBenchmarkMetric(100_000_000.0, 8.4));
		totalMetrics.put("q9", new JobBenchmarkMetric(1_000_000_000.0, 8.4));
		totalMetrics.put("q10", new JobBenchmarkMetric(10_000_000_000.0, 8.4));
		totalMetrics.put("q11", new JobBenchmarkMetric(100_000_000_000.0, 8.419));
		totalMetrics.put("q12", new JobBenchmarkMetric(100_000_000.0, 8.4));
		totalMetrics.put("q13", new JobBenchmarkMetric(10_000_000.0, 8.4));
		totalMetrics.put("q14", new JobBenchmarkMetric(1_000_000.0, 8.4));
		Benchmark.printSummary(totalMetrics);
	}

}