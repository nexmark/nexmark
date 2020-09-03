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

import org.apache.flink.configuration.Configuration;

import com.github.nexmark.flink.metric.BenchmarkMetric;
import com.github.nexmark.flink.metric.FlinkRestClient;
import com.github.nexmark.flink.metric.MetricReporter;
import com.github.nexmark.flink.metric.cpu.CpuMetricReceiver;
import com.github.nexmark.flink.utils.NexmarkGlobalConfiguration;
import com.github.nexmark.flink.workload.Workload;
import com.github.nexmark.flink.workload.WorkloadSuite;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The entry point to run benchmark for nexmark queries.
 */
public class Benchmark {

	// TODO: remove this once q6 is supported
	private static final Set<String> UNSUPPORTED_QUERIES = Collections.singleton("q6");

	private static final Option LOCATION = new Option("l", "location", true,
		"nexmark directory.");
	private static final Option QUERIES = new Option("q", "queries", true,
		"Query to run. If the value is 'all', all queries will be run.");

	public static void main(String[] args) throws ParseException {
		if (args == null || args.length == 0) {
			throw new RuntimeException("Usage: --queries q1,q3 --location /path/to/nexmark");
		}
		Options options = getOptions();
		DefaultParser parser = new DefaultParser();
		CommandLine line = parser.parse(options, args, true);
		Path location = new File(line.getOptionValue(LOCATION.getOpt())).toPath();
		Path queryLocation = location.resolve("queries");
		List<String> queries = getQueries(queryLocation, line.getOptionValue(QUERIES.getOpt()));
		System.out.println("Benchmark Queries: " + queries);
		runQueries(queries, location);
	}

	private static void runQueries(List<String> queries, Path location) {
		String flinkHome = System.getenv("FLINK_HOME");
		if (flinkHome == null) {
			throw new IllegalArgumentException("FLINK_HOME environment variable is not set.");
		}
		Path flinkDist = new File(flinkHome).toPath();

		// start metric servers
		Configuration nexmarkConf = NexmarkGlobalConfiguration.loadConfiguration();
		String jmAddress = nexmarkConf.get(FlinkNexmarkOptions.FLINK_REST_ADDRESS);
		int jmPort = nexmarkConf.get(FlinkNexmarkOptions.FLINK_REST_PORT);
		String reporterAddress = nexmarkConf.get(FlinkNexmarkOptions.METRIC_REPORTER_HOST);
		int reporterPort = nexmarkConf.get(FlinkNexmarkOptions.METRIC_REPORTER_PORT);
		FlinkRestClient flinkRestClient = new FlinkRestClient(jmAddress, jmPort);
		CpuMetricReceiver cpuMetricReceiver = new CpuMetricReceiver(reporterAddress, reporterPort);
		cpuMetricReceiver.runServer();

		Duration monitorDelay = nexmarkConf.get(FlinkNexmarkOptions.METRIC_MONITOR_DELAY);
		Duration monitorInterval = nexmarkConf.get(FlinkNexmarkOptions.METRIC_MONITOR_INTERVAL);
		Duration monitorDuration = nexmarkConf.get(FlinkNexmarkOptions.METRIC_MONITOR_DURATION);

		WorkloadSuite workloadSuite = WorkloadSuite.fromConf(nexmarkConf);

		// start to run queries
		LinkedHashMap<String, BenchmarkMetric> totalMetrics = new LinkedHashMap<>();
		for (String queryName : queries) {
			Workload workload = workloadSuite.getQueryWorkload(queryName);
			if (workload == null) {
				throw new IllegalArgumentException(
					String.format("The workload of query %s is not defined.", queryName));
			}
			MetricReporter reporter = new MetricReporter(
				flinkRestClient,
				cpuMetricReceiver,
				monitorDelay,
				monitorInterval,
				monitorDuration);
			QueryRunner runner = new QueryRunner(
				queryName,
				workload,
				location,
				flinkDist,
				reporter,
				flinkRestClient);
			BenchmarkMetric metric = runner.run();
			totalMetrics.put(queryName, metric);
		}

		// print benchmark summary
		printSummary(totalMetrics);

		flinkRestClient.close();
		cpuMetricReceiver.close();
	}

	/**
	 * Returns the mapping from query name to query file path.
	 */
	private static List<String> getQueries(Path queryLocation, String queries) {
		List<String> queryList = new ArrayList<>();
		if (queries.equals("all")) {
			File queriesDir = queryLocation.toFile();
			if (!queriesDir.exists()) {
				throw new IllegalArgumentException(
					String.format("The queries dir \"%s\" does not exist.", queryLocation));
			}
			for (int i = 0; i < 100; i++) {
				String queryName = "q" + i;
				if (UNSUPPORTED_QUERIES.contains(queryName)) {
					continue;
				}
				File queryFile = new File(queryLocation.toFile(), queryName + ".sql");
				if (queryFile.exists()) {
					queryList.add(queryName);
				}
			}
		} else {
			for (String queryName : queries.split(",")) {
				if (UNSUPPORTED_QUERIES.contains(queryName)) {
					continue;
				}
				File queryFile = new File(queryLocation.toFile(), queryName + ".sql");
				if (!queryFile.exists()) {
					throw new IllegalArgumentException(
						String.format("The query path \"%s\" does not exist.", queryFile.getAbsolutePath()));
				}
				queryList.add(queryName);
			}
		}
		return queryList;
	}

	public static void printSummary(LinkedHashMap<String, BenchmarkMetric> totalMetrics) {
		if (totalMetrics.isEmpty()) {
			return;
		}
		System.err.println("-------------------------------- Nexmark Results --------------------------------");
		int itemMaxLength = 20;
		System.err.println();
		printLine('-', "+", itemMaxLength, "", "", "", "");
		printLine(' ', "|", itemMaxLength, " Nexmark Query", " Throughput (r/s)", " Cores", " Throughput/Cores");
		printLine('-', "+", itemMaxLength, "", "", "", "");

		for (Map.Entry<String, BenchmarkMetric> entry : totalMetrics.entrySet()) {
			BenchmarkMetric metric = entry.getValue();
			printLine(' ', "|", itemMaxLength,
				entry.getKey(),
				metric.getPrettyTps(),
				metric.getPrettyCpu(),
				metric.getPrettyTpsPerCore());
		}
		printLine('-', "+", itemMaxLength, "", "", "", "");
		System.err.println();
	}

	private static void printLine(
		char charToFill,
		String separator,
		int itemMaxLength,
		String... items) {
		StringBuilder builder = new StringBuilder();
		for (String item : items) {
			builder.append(separator);
			builder.append(item);
			int left = itemMaxLength - item.length() - separator.length();
			for (int i = 0; i < left; i++) {
				builder.append(charToFill);
			}
		}
		builder.append(separator);
		System.err.println(builder.toString());
	}

	private static Options getOptions() {
		Options options = new Options();
		options.addOption(QUERIES);
		options.addOption(LOCATION);
		return options;
	}
}
