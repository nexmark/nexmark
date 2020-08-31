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

import com.github.nexmark.flink.metric.tps.TpsMetric;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class FlinkRestClientTest {

	@Test
	public void testMetricsClient() {
		FlinkRestClient client = new FlinkRestClient("localhost", 8081);
		String jobId = client.getCurrentJobId();
		System.out.println("jobId: " + jobId);

		String vertexId = client.getSourceVertexId(jobId);
		System.out.println("vertexId: " + vertexId);

		String metricName = client.getTpsMetricName(jobId, vertexId);
		System.out.println("metricName: " + metricName);

		TpsMetric tps = client.getTpsMetric(jobId, vertexId, metricName);
		System.out.println("tps: " + tps);
	}

	@Test
	public void testCancelJob() {
		FlinkRestClient client = new FlinkRestClient("localhost", 8081);
		String jobId = client.getCurrentJobId();
		System.out.println("jobId: " + jobId);
		client.cancelJob(jobId);
	}

}
