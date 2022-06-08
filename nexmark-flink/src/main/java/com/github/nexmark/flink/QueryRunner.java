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

import com.github.nexmark.flink.metric.FlinkRestClient;
import com.github.nexmark.flink.metric.JobBenchmarkMetric;
import com.github.nexmark.flink.metric.MetricReporter;
import com.github.nexmark.flink.utils.AutoClosableProcess;
import com.github.nexmark.flink.workload.Workload;
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

import static com.github.nexmark.flink.Benchmark.CATEGORY_OA;

public class QueryRunner {

	private static final Logger LOG = LoggerFactory.getLogger(QueryRunner.class);

	private final String queryName;
	private final Workload workload;
	private final Path location;
	private final Path queryLocation;
	private final Path flinkDist;
	private final MetricReporter metricReporter;
	private final FlinkRestClient flinkRestClient;

	public QueryRunner(String queryName, Workload workload, Path location, Path flinkDist, MetricReporter metricReporter, FlinkRestClient flinkRestClient, String category) {
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
			System.out.println("==================================================================");
			System.out.println("Start to run query " + queryName + " with workload " + workload.getSummaryString());
			LOG.info("==================================================================");
			LOG.info("Start to run query " + queryName + " with workload " + workload.getSummaryString());
			if (!"insert_kafka".equals(queryName) // no warmup for kafka source prepare
					&& (workload.getWarmupMills() > 0L || workload.getKafkaServers() == null)  // when using kafka source we need a stop for warmup
					&& ((workload.getWarmupTps() > 0L && workload.getWarmupEvents() > 0L) || workload.getKafkaServers() != null) // otherwise we need a configuration for datagen source
			) {
				System.out.println("Start the warmup for at most " + workload.getWarmupMills() + "ms and " + workload.getWarmupEvents() + " events.");
				LOG.info("Start the warmup for at most " + workload.getWarmupMills() + "ms and " + workload.getWarmupEvents() + " events.");
				runWarmup(workload.getWarmupTps(), workload.getWarmupEvents());
				long waited = waitForOrJobFinish(workload.getWarmupMills());
				waited += cancelJob();
				System.out.println("Stop the warmup, cost " + waited + "ms.");
				LOG.info("Stop the warmup, cost " + waited + ".");
			}
			runInternal();
			// blocking until collect enough metrics
			String jobId = flinkRestClient.getCurrentJobId();
			JobBenchmarkMetric metrics = metricReporter.reportMetric(jobId, workload.getEventsNum());
			// cancel job
			System.out.println("Stop job query " + queryName);
			LOG.info("Stop job query " + queryName);
			cancelJob();
			return metrics;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private long waitForOrJobFinish(long mills) {
		long waited = 0L;
		while ((mills <= 0L || waited < mills) && flinkRestClient.isJobRunning()) {
			try {
				Thread.sleep(100L);
				waited += 100L;
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		return waited;
	}

	private long cancelJob() {
		long cost = 0L;
		while (!flinkRestClient.isJobCancellingOrFinished()) {
			// make sure the job is canceled.
			flinkRestClient.cancelJob(flinkRestClient.getCurrentJobId());
			try {
				Thread.sleep(100L);
				cost += 100L;
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		return cost;
	}

	private void runWarmup(long tps, long events) throws IOException {
		Map<String, String> varsMap = initializeVarsMap();
		if (workload.getKafkaServers() == null) {
			varsMap.put("TPS", String.valueOf(tps));
			varsMap.put("EVENTS_NUM", String.valueOf(events));
		}
		List<String> sqlLines = initializeAllSqlLines(varsMap);
		submitSQLJob(sqlLines);
	}

	private void runInternal() throws IOException {
		Map<String, String> varsMap = initializeVarsMap();
		List<String> sqlLines = initializeAllSqlLines(varsMap);
		submitSQLJob(sqlLines);
	}

	private Map<String, String> initializeVarsMap() {
		LocalDateTime currentTime = LocalDateTime.now();
		LocalDateTime submitTime = currentTime.minusNanos(currentTime.getNano());

		Map<String, String> varsMap = new HashMap<>();
		varsMap.put("NEXMARK_DIR", location.toFile().getAbsolutePath());
		varsMap.put("SUBMIT_TIME", submitTime.toString());
		varsMap.put("FLINK_HOME", flinkDist.toFile().getAbsolutePath());
		varsMap.put("TPS", String.valueOf(workload.getTps()));
		varsMap.put("EVENTS_NUM", String.valueOf(workload.getEventsNum()));
		varsMap.put("PERSON_PROPORTION", String.valueOf(workload.getPersonProportion()));
		varsMap.put("AUCTION_PROPORTION", String.valueOf(workload.getAuctionProportion()));
		varsMap.put("BID_PROPORTION", String.valueOf(workload.getBidProportion()));
		varsMap.put("NEXMARK_TABLE", workload.getKafkaServers() == null ? "datagen" : "kafka");
		varsMap.put("BOOTSTRAP_SERVERS", workload.getKafkaServers() == null ? "" : workload.getKafkaServers());
		return varsMap;
	}

	private List<String> initializeAllSqlLines(Map<String, String> vars) throws IOException {
		List<String> allLines = new ArrayList<>();
		allLines.addAll(initializeSqlFileLines(vars, new File(queryLocation.toFile(), "ddl_gen.sql")));
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

	public void submitSQLJob(List<String> sqlLines) throws IOException {
		Path flinkBin = flinkDist.resolve("bin");
		final List<String> commands = new ArrayList<>();
		commands.add(flinkBin.resolve("sql-client.sh").toAbsolutePath().toString());
		commands.add("embedded");

		LOG.info("\n================================================================================"
				+ "\nQuery {} is running."
				+ "\n--------------------------------------------------------------------------------"
				+ "\n"
			, queryName);

		AutoClosableProcess
			.create(commands.toArray(new String[0]))
			.setStdInputs(sqlLines.toArray(new String[0]))
			.setStdoutProcessor(LOG::info) // logging the SQL statements and error message
			.runBlocking();
	}

}
