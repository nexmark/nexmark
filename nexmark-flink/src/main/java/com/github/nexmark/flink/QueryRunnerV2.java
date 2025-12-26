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

import static com.github.nexmark.flink.Benchmark.CATEGORY_OA;

import com.github.nexmark.flink.metric.FlinkRestClient;
import com.github.nexmark.flink.metric.JobBenchmarkMetric;
import com.github.nexmark.flink.metric.MetricReporter;
import com.github.nexmark.flink.metric.Savepoint;
import com.github.nexmark.flink.utils.AutoClosableProcess;
import com.github.nexmark.flink.workload.Workload;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryRunnerV2 {

	private static final Logger LOG = LoggerFactory.getLogger(QueryRunnerV2.class);

	private final String queryName;
	private final Workload workload;
	private final Path location;
	private final Path queryLocation;
	private final Path flinkDist;
	private final MetricReporter metricReporter;
	private final FlinkRestClient flinkRestClient;

	public QueryRunnerV2(String queryName, Workload workload, Path location, Path flinkDist, MetricReporter metricReporter, FlinkRestClient flinkRestClient, String category) {
		this.queryName = queryName;
		this.workload = workload;
		this.location = location;
		this.queryLocation =
				CATEGORY_OA.equals(category) ? location.resolve("queries") : location.resolve("queries-" + category);
		this.flinkDist = flinkDist;
		this.metricReporter = metricReporter;
		this.flinkRestClient = flinkRestClient;
	}

	public JobBenchmarkMetric run() {
		try {
			Savepoint savepoint = null;
			System.out.println("==================================================================");
			System.out.println("Start to run query " + queryName + " with workload " + workload.getSummaryString());
			LOG.info("==================================================================");
			LOG.info("Start to run query " + queryName + " with workload " + workload.getSummaryString());
			long totalEvents = workload.getWarmupEvents() + workload.getEventsNum();
			System.out.println("Start the warmup for " + workload.getWarmupEvents() + " events.");
			LOG.info("Start the warmup for " + workload.getWarmupEvents() + " events.");
			String warmupJob = runWarmup(workload.getWarmupEvents(), totalEvents);
			long waited = waitForOrJobFinish(warmupJob, workload.getWarmupEvents());
			Tuple2<Savepoint, Long> cancelResult = cancelJob(warmupJob, true);
			savepoint = cancelResult.f0;
			waited += cancelResult.f1;
			System.out.println("Stop the warmup, cost " + waited + "ms.");
			LOG.info("Stop the warmup, cost " + waited + ".");

			if (savepoint == null) {
				System.out.println("The query set warmup with savepoint, but does not get any savepoint.");
				LOG.error("The query set warmup with savepoint, but does not get any savepoint.");
				throw new RuntimeException("The query set warmup with savepoint, but does not get any savepoint.");
			} else {
				System.out.println("Get warmup savepoint: " + savepoint);
				LOG.info("Get warmup savepoint: " + savepoint);
			}


			String jobId = runInternal(totalEvents, savepoint);
			// blocking until collect enough metrics
			JobBenchmarkMetric metrics = metricReporter.reportMetric(jobId,
                    workload.getEventsNum(),
                    false,
                    workload.getKafkaServers() != null);
			// cancel job
			System.out.println("Stop job query " + queryName);
			LOG.info("Stop job query " + queryName);
			cancelJob(jobId, false);
			return metrics;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private long waitForOrJobFinish(String jobId, long recordLimit) {
		long start = System.currentTimeMillis();
		while (flinkRestClient.isJobRunning(jobId, recordLimit)) {
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		return System.currentTimeMillis() - start;
	}

	private Tuple2<Savepoint, Long> cancelJob(String jobId, boolean savepoint) {
		System.out.println("Cancelling job " + jobId + " with checkpoint = " + savepoint);
		long start = System.currentTimeMillis();
		boolean triggered = false;
		String requestId = null;

		while (!flinkRestClient.isJobCanceledOrFinished(jobId)) {
			// make sure the job is canceled.
			if (savepoint) {
				if (!triggered) {
					requestId = flinkRestClient.triggerCheckpoint(jobId);
					triggered = true;
				} else {
					Savepoint.Status status = flinkRestClient.checkCheckpointFinished(jobId, requestId);
					if (status == Savepoint.Status.COMPLETED) {
						// just wait for finished.
						flinkRestClient.cancelJob(jobId);
					} else if (status == Savepoint.Status.FAILED) {
						triggered = false;
					}
				}
			} else {
				flinkRestClient.cancelJob(jobId);
			}
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		return Tuple2.of(
				savepoint ? flinkRestClient.getJobLastCheckpoint(jobId) : null,
				System.currentTimeMillis() - start
		);
	}

	private String runWarmup(long stopAtEvents, long totalEvents) throws IOException {
		Map<String, String> varsMap = initializeVarsMap();
		varsMap.put("EVENTS_NUM", String.valueOf(totalEvents));
		varsMap.put("STOP_AT", String.valueOf(stopAtEvents));
		varsMap.put("KEEP_ALIVE", "true");
		List<String> sqlLines = initializeAllSqlLines(varsMap, queryName + "_warmup", null);
		return submitSQLJob(sqlLines);
	}

	private String runInternal(long totalEvents, Savepoint savepoint) throws IOException {
		Map<String, String> varsMap = initializeVarsMap();
		varsMap.put("EVENTS_NUM", String.valueOf(totalEvents));
		varsMap.put("STOP_AT", "-1");
		varsMap.put("KEEP_ALIVE", "false");
		List<String> sqlLines = initializeAllSqlLines(varsMap, queryName, savepoint);
		return submitSQLJob(sqlLines);
	}

	private Map<String, String> initializeVarsMap() {
		LocalDateTime currentTime = LocalDateTime.now();
		LocalDateTime submitTime = currentTime.minusNanos(currentTime.getNano());

		Map<String, String> varsMap = new HashMap<>();
		varsMap.put("NEXMARK_DIR", location.toFile().getAbsolutePath());
		varsMap.put("SUBMIT_TIME", submitTime.toString());
		varsMap.put("FLINK_HOME", flinkDist.toFile().getAbsolutePath());
		varsMap.put("TPS", String.valueOf(workload.getTps()));
		varsMap.put("PERSON_PROPORTION", String.valueOf(workload.getPersonProportion()));
		varsMap.put("AUCTION_PROPORTION", String.valueOf(workload.getAuctionProportion()));
		varsMap.put("BID_PROPORTION", String.valueOf(workload.getBidProportion()));
		varsMap.put("NEXMARK_TABLE", "datagen");
		return varsMap;
	}

	private List<String> initializeAllSqlLines(Map<String, String> vars, String name, Savepoint savepoint) throws IOException {
		List<String> allLines = new ArrayList<>();
		if (savepoint != null) {
			allLines.add("SET 'execution.savepoint.path' = '" + savepoint.getPath() + "';");
			allLines.add("SET 'execution.state-recovery.claim-mode' = 'CLAIM';");
		}
		if (name != null && !name.isEmpty()) {
			allLines.add("SET 'pipeline.name' = '" + name + "';");
		}
		allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), "ddl_gen_v2.sql")));
		allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), "ddl_kafka.sql")));
		allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), "ddl_views.sql")));
		allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), queryName + ".sql")));
		return allLines;
	}

	private List<String> initializeSqlFileLines(Map<String, String> vars, File sqlFile) throws IOException {
		List<String> lines = Files.readAllLines(sqlFile.toPath());
		List<String> result = new ArrayList<>();
		for (String line : lines) {
			for (Map.Entry<String, String> var : vars.entrySet()) {
				line = line.replace("${" + var.getKey() + "}", var.getValue());
			}
			result.add(line);
		}
		return result;
	}

	public String submitSQLJob(List<String> sqlLines) throws IOException {
		Path flinkBin = flinkDist.resolve("bin");
		final List<String> commands = new ArrayList<>();
		commands.add(flinkBin.resolve("sql-client.sh").toAbsolutePath().toString());
		commands.add("embedded");

		LOG.info("\n================================================================================"
				+ "\nQuery {} is running."
				+ "\n--------------------------------------------------------------------------------"
				+ "\n"
			, queryName);

		StringBuilder output = new StringBuilder();
		AutoClosableProcess
				.create(commands.toArray(new String[0]))
				.setStdInputs(sqlLines.toArray(new String[0]))
				.setStdoutProcessor(output::append) // logging the SQL statements and error message
				.runBlocking();

		Pattern pattern = Pattern.compile("Job ID: ([A-Za-z0-9]{32})");
		Matcher matcher = pattern.matcher(output.toString());
		if (matcher.find()) {
			return matcher.group(1);
		} else {
			throw new RuntimeException("Cannot find Job ID from the sql client output, maybe the job is not successfully submitted.");
		}
	}

}
