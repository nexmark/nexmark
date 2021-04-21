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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetricReporterTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testEventsNumModeWithMonitorDuration() {
		exception.expectMessage(
				"The configuration of monitorDuration is not supported in the events number mode");

		FlinkRestClient client = mock(FlinkRestClient.class);
		when(client.getCurrentJobId()).thenReturn("");
		when(client.getSourceVertexId(any())).thenReturn("");
		when(client.getTpsMetricName(any(), any())).thenReturn("");
		MetricReporter reporter = new MetricReporter(
				client,
				null,
				Duration.ofMillis(1),
				Duration.ofMillis(10),
				Duration.ofMillis(100));
		reporter.reportMetric(100);
	}
}
