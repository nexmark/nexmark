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

import com.github.nexmark.flink.metric.FlinkRestClient;
import com.github.nexmark.flink.metric.JobBenchmarkMetric;
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

import static com.github.nexmark.flink.metric.BenchmarkMetric.NUMBER_FORMAT;
import static com.github.nexmark.flink.metric.BenchmarkMetric.formatDoubleValue;
import static com.github.nexmark.flink.metric.BenchmarkMetric.formatLongValue;
import static com.github.nexmark.flink.metric.BenchmarkMetric.formatLongValuePerSecond;

/**
 * The entry point to run benchmark for nexmark queries.
 */
public class Benchmark {

	// TODO: remove this once q6 is supported
	private static final Set<String> UNSUPPORTED_QUERIES = Collections.singleton("q6");

	private static final Option LOCATION = new Option("l", "location", true,
		"Nexmark directory.");
	private static final Option QUERIES = new Option("q", "queries", true,
		"Query to run. If the value is 'all', all queries will be run.");
	private static final Option CATEGORY = new Option("c", "category", true,
			"Query category.");

	public static final String CATEGORY_OA = "oa";

	public static void main(String[] args) throws ParseException {
		if (args == null || args.length == 0) {
			throw new RuntimeException("Usage: --queries q1,q3 --category oa --location /path/to/nexmark");
		}
		Options options = getOptions();
		DefaultParser parser = new DefaultParser();
		CommandLine line = parser.parse(options, args, true);
		Path location = new File(line.getOptionValue(LOCATION.getOpt())).toPath();
		String category = CATEGORY.getValue(CATEGORY_OA).toLowerCase();
		boolean isQueryOa = CATEGORY_OA.equals(category);
		Path queryLocation = isQueryOa ? location.resolve("queries") : location.resolve("queries-" + category);
		List<String> queries = getQueries(queryLocation, line.getOptionValue(QUERIES.getOpt()), isQueryOa);
		System.out.println("Benchmark Queries: " + queries);
		runQueries(queries, location, category);
	}

	private static void runQueries(List<String> queries, Path location, String category) {
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

		WorkloadSuite workloadSuite = WorkloadSuite.fromConf(nexmarkConf, category);

		// start to run queries
		LinkedHashMap<String, JobBenchmarkMetric> totalMetrics = new LinkedHashMap<>();
		executeQueries(
				queries,
				workloadSuite,
				flinkRestClient,
				cpuMetricReceiver,
				monitorDelay,
				monitorInterval,
				monitorDuration,
				location,
				flinkDist,
				totalMetrics,
				category);

		// print benchmark summary
		printSummary(totalMetrics);

		flinkRestClient.close();
		cpuMetricReceiver.close();
	}

	/**
	 * Returns the mapping from query name to query file path.
	 */
	private static List<String> getQueries(Path queryLocation, String queries, boolean isQueryOa) {
		List<String> queryList = new ArrayList<>();
		if (queries.equals("all")) {
			File queriesDir = queryLocation.toFile();
			if (!queriesDir.exists()) {
				throw new IllegalArgumentException(
					String.format("The queries dir \"%s\" does not exist.", queryLocation));
			}
			for (int i = 0; i < 100; i++) {
				String queryName = "q" + i;
				if (isQueryOa && UNSUPPORTED_QUERIES.contains(queryName)) {
					continue;
				}
				File queryFile = new File(queryLocation.toFile(), queryName + ".sql");
				if (queryFile.exists()) {
					queryList.add(queryName);
				}
			}
		} else {
			for (String queryName : queries.split(",")) {
				if (isQueryOa && UNSUPPORTED_QUERIES.contains(queryName)) {
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

	private static void executeQueries(
			List<String> queries,
			WorkloadSuite workloadSuite,
			FlinkRestClient flinkRestClient,
			CpuMetricReceiver cpuMetricReceiver,
			Duration monitorDelay,
			Duration monitorInterval,
			Duration monitorDuration,
			Path location,
			Path flinkDist,
			LinkedHashMap<String, JobBenchmarkMetric> totalMetrics,
			String category) {
		for (String queryName : queries) {
			Workload workload = workloadSuite.getQueryWorkload(queryName);
			if (workload == null) {
				throw new IllegalArgumentException(
						String.format("The workload of query %s is not defined.", queryName));
			}
			workload.validateWorkload(monitorDuration);

			MetricReporter reporter =
					new MetricReporter(
							flinkRestClient,
							cpuMetricReceiver,
							monitorDelay,
							monitorInterval,
							monitorDuration);
			QueryRunner runner =
					new QueryRunner(
							queryName,
							workload,
							location,
							flinkDist,
							reporter,
							flinkRestClient,
							category);
			JobBenchmarkMetric metric = runner.run();
			totalMetrics.put(queryName, metric);
		}
	}

	public static void printSummary(LinkedHashMap<String, JobBenchmarkMetric> totalMetrics) {
		if (totalMetrics.isEmpty()) {
			return;
		}
		System.err.println("-------------------------------- Nexmark Results --------------------------------");
		int itemMaxLength = 20;
		System.err.println();
		if (totalMetrics.values().iterator().next().getEventsNum() != 0) {
			printEventNumSummary(itemMaxLength, totalMetrics);
		} else {
			printTPSSummary(itemMaxLength, totalMetrics);
		}
		System.err.println();
	}

	private static void printEventNumSummary(
			int itemMaxLength, LinkedHashMap<String, JobBenchmarkMetric> totalMetrics) {
		printLine('-', "+", itemMaxLength, "", "", "", "", "", "");
		printLine(' ', "|", itemMaxLength, " Nexmark Query", " Events Num", " Cores", " Time(s)", " Cores * Time(s)", " Throughput/Cores");
		printLine('-', "+", itemMaxLength, "", "", "", "", "", "");

		long totalEventsNum = 0;
		double totalCpus = 0;
		double totalTimeSeconds = 0;
		double totalCoresMultiplyTimeSeconds = 0;
		double totalThroughputPerCore = 0;
		for (Map.Entry<String, JobBenchmarkMetric> entry : totalMetrics.entrySet()) {
			JobBenchmarkMetric metric = entry.getValue();
			double throughputPerCore = metric.getEventsNum() / metric.getCoresMultiplyTimeSeconds();
			printLine(' ', "|", itemMaxLength,
					entry.getKey(),
					NUMBER_FORMAT.format(metric.getEventsNum()),
					NUMBER_FORMAT.format(metric.getCpu()),
					formatDoubleValue(metric.getTimeSeconds()),
					formatDoubleValue(metric.getCoresMultiplyTimeSeconds()),
					formatLongValuePerSecond((long) throughputPerCore));
			totalEventsNum += metric.getEventsNum();
			totalCpus += metric.getCpu();
			totalTimeSeconds += metric.getTimeSeconds();
			totalCoresMultiplyTimeSeconds += metric.getCoresMultiplyTimeSeconds();
			totalThroughputPerCore += throughputPerCore;
		}
		printLine(' ', "|", itemMaxLength,
				"Total",
				NUMBER_FORMAT.format(totalEventsNum),
				formatDoubleValue(totalCpus),
				formatDoubleValue(totalTimeSeconds),
				formatDoubleValue(totalCoresMultiplyTimeSeconds),
				formatLongValuePerSecond((long) totalThroughputPerCore));
		printLine('-', "+", itemMaxLength, "", "", "", "", "", "");
	}

	private static void printTPSSummary(
			int itemMaxLength, LinkedHashMap<String, JobBenchmarkMetric> totalMetrics) {
		printLine('-', "+", itemMaxLength, "", "", "", "");
		printLine(' ', "|", itemMaxLength, " Nexmark Query", " Throughput (r/s)", " Cores", " Throughput/Cores");
		printLine('-', "+", itemMaxLength, "", "", "", "");

		long totalTpsPerCore = 0;
		for (Map.Entry<String, JobBenchmarkMetric> entry : totalMetrics.entrySet()) {
			JobBenchmarkMetric metric = entry.getValue();
			printLine(' ', "|", itemMaxLength,
				entry.getKey(),
				metric.getPrettyTps(),
				metric.getPrettyCpu(),
				metric.getPrettyTpsPerCore());
			totalTpsPerCore += metric.getTpsPerCore();
		}
		printLine(' ', "|", itemMaxLength,
				"Total",
				"-",
				"-",
				formatLongValue(totalTpsPerCore));
		printLine('-', "+", itemMaxLength, "", "", "", "");
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
		options.addOption(CATEGORY);
		options.addOption(LOCATION);
		return options;
	}
}
