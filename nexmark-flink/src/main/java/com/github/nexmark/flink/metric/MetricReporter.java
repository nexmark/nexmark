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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.java.tuple.Tuple3;

import com.github.nexmark.flink.metric.cpu.CpuMetricReceiver;
import com.github.nexmark.flink.metric.tps.TpsMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A reporter to aggregate metrics and report summary results.
 */
public class MetricReporter {

	private static final Logger LOG = LoggerFactory.getLogger(MetricReporter.class);

	private final Duration monitorDelay;
	private final Duration monitorInterval;
	private final Duration monitorDuration;
	private final FlinkRestClient flinkRestClient;
	private final CpuMetricReceiver cpuMetricReceiver;
	private final List<BenchmarkMetric> metrics;
	private final ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
	private volatile Throwable error;

	public MetricReporter(FlinkRestClient flinkRestClient, CpuMetricReceiver cpuMetricReceiver, Duration monitorDelay, Duration monitorInterval, Duration monitorDuration) {
		this.monitorDelay = monitorDelay;
		this.monitorInterval = monitorInterval;
		this.monitorDuration = monitorDuration;
		this.flinkRestClient = flinkRestClient;
		this.cpuMetricReceiver = cpuMetricReceiver;
		this.metrics = new ArrayList<>();
	}

	private void submitMonitorThread() {

		String jobId;
		String vertexId;
		String metricName;

		while (true) {
			Tuple3<String, String, String> jobInfo = getJobInformation();
			if (jobInfo != null) {
				jobId = jobInfo.f0;
				vertexId = jobInfo.f1;
				metricName = jobInfo.f2;
				break;
			} else {
				// wait for the job startup
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}

		this.service.scheduleWithFixedDelay(
			new MetricCollector(jobId, vertexId, metricName),
			0L,
			monitorInterval.toMillis(),
			TimeUnit.MILLISECONDS
		);
	}

	private Tuple3<String, String, String> getJobInformation() {
		try {
			String jobId = flinkRestClient.getCurrentJobId();
			String vertexId = flinkRestClient.getSourceVertexId(jobId);
			String metricName = flinkRestClient.getTpsMetricName(jobId, vertexId);
			return Tuple3.of(jobId, vertexId, metricName);
		} catch (Exception e) {
			LOG.warn("Job metric is not ready yet.", e);
			return null;
		}
	}

	private void waitFor(Duration duration) {
		Deadline deadline = Deadline.fromNow(duration);
		while (deadline.hasTimeLeft()) {
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			if (error != null) {
				throw new RuntimeException(error);
			}
		}
	}

	public BenchmarkMetric reportMetric() {
		System.out.println(String.format("Monitor metrics after %s minutes.", monitorDelay.toMinutes()));
		waitFor(monitorDelay);
		System.out.println(String.format("Start to monitor metrics for %s minutes.", monitorDuration.toMinutes()));
		submitMonitorThread();
		waitFor(monitorDuration);

		// cleanup the resource
		this.close();

		if (metrics.isEmpty()) {
			throw new RuntimeException("The metric reporter doesn't collect any metrics.");
		}
		double sumTps = 0.0;
		double sumCpu = 0.0;

		for (BenchmarkMetric metric : metrics) {
			sumTps += metric.getTps();
			sumCpu += metric.getCpu();
		}

		double avgTps = sumTps / metrics.size();
		double avgCpu = sumCpu / metrics.size();
		BenchmarkMetric metric = new BenchmarkMetric(avgTps, avgCpu);
		String message = String.format("Summary Average: Throughput=%s, Cores=%s",
			metric.getPrettyTps(),
			metric.getPrettyCpu());
		System.out.println(message);
		LOG.info(message);
		return metric;
	}

	public void close() {
		service.shutdownNow();
	}

	private class MetricCollector implements Runnable {
		private final String jobId;
		private final String vertexId;
		private final String metricName;

		private MetricCollector(String jobId, String vertexId, String metricName) {
			this.jobId = jobId;
			this.vertexId = vertexId;
			this.metricName = metricName;
		}

		@Override
		public void run() {
			try {
				TpsMetric tps = flinkRestClient.getTpsMetric(jobId, vertexId, metricName);
				double cpu = cpuMetricReceiver.getTotalCpu();
				int tms = cpuMetricReceiver.getNumberOfTM();
				BenchmarkMetric metric = new BenchmarkMetric(tps.getSum(), cpu);
				// it's thread-safe to update metrics
				metrics.add(metric);
				// logging
				String message = String.format("Current Throughput=%s, Cores=%s (%s TMs)",
					metric.getPrettyTps(),
					metric.getPrettyCpu(),
					tms);
				System.out.println(message);
				LOG.info(message);
			} catch (Exception e) {
				error = e;
			}
		}
	}
}
