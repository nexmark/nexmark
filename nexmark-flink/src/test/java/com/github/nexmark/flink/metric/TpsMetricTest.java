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
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TpsMetricTest {

	@Test
	public void testParseJson() {
		String json = "[\n" +
			"{\n" +
			"\"id\": \"Source__TableSourceScan(table=[[default_catalog__default_database__nexmark]]__fi.numRecordsOutPerSecond\",\n" +
			"\"min\": 5003.2,\n" +
			"\"max\": 5003.2,\n" +
			"\"avg\": 5003.2,\n" +
			"\"sum\": 10006.3\n" +
			"}\n" +
			"]";

		TpsMetric tps = TpsMetric.fromJson(json);
		TpsMetric expected = new TpsMetric(
			"Source__TableSourceScan(table=[[default_catalog__default_database__nexmark]]__fi.numRecordsOutPerSecond",
			5003.2,
			5003.2,
			5003.2,
			10006.3);
		assertEquals(expected, tps);
	}
}
