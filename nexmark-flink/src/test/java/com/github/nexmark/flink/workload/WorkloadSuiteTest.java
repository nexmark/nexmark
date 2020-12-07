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

package com.github.nexmark.flink.workload;

import org.apache.flink.configuration.Configuration;

import com.github.nexmark.flink.utils.NexmarkGlobalConfiguration;
import com.github.nexmark.flink.utils.NexmarkGlobalConfigurationTest;
import org.junit.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class WorkloadSuiteTest {

	@Test
	public void testCustomizedConf() {
		Configuration conf = new Configuration();
		conf.setString("nexmark.workload.suite.8m.tps", "8000000");
		conf.setString("nexmark.workload.suite.8m.queries", "q0,q1,q2,q10,q12,q13,q14");
		conf.setString("nexmark.workload.suite.2m-no-bid.tps", "2000000");
		conf.setString("nexmark.workload.suite.2m-no-bid.percentage", "bid:0, auction:9, person:1");
		conf.setString("nexmark.workload.suite.2m-no-bid.queries", "q3,q8");
		conf.setString("nexmark.workload.suite.2m.tps", "2000000");
		conf.setString("nexmark.workload.suite.2m.queries", "q5,q15");
		conf.setString("nexmark.workload.suite.1m.tps", "1000000");
		conf.setString("nexmark.workload.suite.1m.queries", "q4,q7,q9,q11");
		WorkloadSuite suite = WorkloadSuite.fromConf(conf);

		Workload load8m = new Workload(8000000, 1, 3, 46);
		Workload load2mNoBid = new Workload(2000000, 1, 9, 0);
		Workload load2m = new Workload(2000000, 1, 3, 46);
		Workload load1m = new Workload(1000000, 1, 3, 46);

		Map<String, Workload> query2Workload = new HashMap<>();
		query2Workload.put("q0", load8m);
		query2Workload.put("q1", load8m);
		query2Workload.put("q2", load8m);
		query2Workload.put("q10", load8m);
		query2Workload.put("q12", load8m);
		query2Workload.put("q13", load8m);
		query2Workload.put("q14", load8m);

		query2Workload.put("q3", load2mNoBid);
		query2Workload.put("q8", load2mNoBid);

		query2Workload.put("q5", load2m);

		query2Workload.put("q4", load1m);
		query2Workload.put("q7", load1m);
		query2Workload.put("q9", load1m);
		query2Workload.put("q11", load1m);
		query2Workload.put("q15", load2m);

		WorkloadSuite expected = new WorkloadSuite(query2Workload);

		assertEquals(expected, suite);
	}

	@Test
	public void testDefaultConf() {
		URL confDir = NexmarkGlobalConfigurationTest.class.getClassLoader().getResource("conf");
		assert confDir != null;
		Configuration conf = NexmarkGlobalConfiguration.loadConfiguration(confDir.getPath());
		WorkloadSuite suite = WorkloadSuite.fromConf(conf);

		Workload load = new Workload(10000000, 1, 3, 46);

		Map<String, Workload> query2Workload = new HashMap<>();
		query2Workload.put("q0", load);
		query2Workload.put("q1", load);
		query2Workload.put("q2", load);
		query2Workload.put("q3", load);
		query2Workload.put("q4", load);
		query2Workload.put("q5", load);
		query2Workload.put("q7", load);
		query2Workload.put("q8", load);
		query2Workload.put("q9", load);
		query2Workload.put("q10", load);
		query2Workload.put("q11", load);
		query2Workload.put("q12", load);
		query2Workload.put("q13", load);
		query2Workload.put("q14", load);
		query2Workload.put("q15", load);

		WorkloadSuite expected = new WorkloadSuite(query2Workload);

		assertEquals(expected, suite);
	}
}