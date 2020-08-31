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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import com.github.nexmark.flink.metric.cpu.CpuMetric;
import com.github.nexmark.flink.utils.NexmarkUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CpuMetricTest {

	@Test
	public void testCpuMetric() throws JsonProcessingException {
		List<CpuMetric> cpuMetrics = new ArrayList<>();
		cpuMetrics.add(new CpuMetric("10.0.0.12", 37927, 1.01));
		cpuMetrics.add(new CpuMetric("10.1.0.33", 54389, 2.3));
		cpuMetrics.add(new CpuMetric("10.2.0.44", 4401, 0.4));
		String result = NexmarkUtils.MAPPER.writeValueAsString(cpuMetrics);

		List<CpuMetric> expected = CpuMetric.fromJsonArray(result);
		assertEquals(expected, cpuMetrics);
	}


}
