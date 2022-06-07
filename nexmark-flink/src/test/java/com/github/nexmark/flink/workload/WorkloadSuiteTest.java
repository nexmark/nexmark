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

import com.github.nexmark.flink.FlinkNexmarkOptions;
import com.github.nexmark.flink.utils.NexmarkGlobalConfiguration;
import com.github.nexmark.flink.utils.NexmarkGlobalConfigurationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.github.nexmark.flink.Benchmark.CATEGORY_OA;
import static org.junit.Assert.assertEquals;

public class WorkloadSuiteTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private static final String CATEGORY_CEP = "cep";

	@Test
	public void testCustomizedConf() {
		Configuration conf = new Configuration();
		conf.setString("nexmark.workload.suite.8m.tps", "8000000");
		conf.setString("nexmark.workload.suite.8m.events.num", "80000000");
		conf.setString("nexmark.workload.suite.8m.queries", "q0,q1,q2,q10,q12,q13,q14");
		conf.setString("nexmark.workload.suite.8m.queries.cep", "q0");
		conf.setString("nexmark.workload.suite.2m-no-bid.tps", "2000000");
		conf.setString("nexmark.workload.suite.2m-no-bid.percentage", "bid:0, auction:9, person:1");
		conf.setString("nexmark.workload.suite.2m-no-bid.queries", "q3,q8");
		conf.setString("nexmark.workload.suite.2m-no-bid.queries.cep", "q1");
		conf.setString("nexmark.workload.suite.2m.tps", "2000000");
		conf.setString("nexmark.workload.suite.2m.queries", "q5,q15");
		conf.setString("nexmark.workload.suite.2m.queries.cep", "q2");
		conf.setString("nexmark.workload.suite.1m.tps", "1000000");
		conf.setString("nexmark.workload.suite.1m.queries", "q4,q7,q9,q11");
		conf.setString("nexmark.workload.suite.1m.queries.cep", "q3");

		Workload load8m = new Workload(8000000, 80000000, 1, 3, 46);
		Workload load2mNoBid = new Workload(2000000, 0, 1, 9, 0);
		Workload load2m = new Workload(2000000, 0, 1, 3, 46);
		Workload load1m = new Workload(1000000, 0, 1, 3, 46);

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

		assertEquals(expected.toString(), WorkloadSuite.fromConf(conf, CATEGORY_OA).toString());

		query2Workload = new HashMap<>();

		query2Workload.put("q0", load8m);

		query2Workload.put("q1", load2mNoBid);

		query2Workload.put("q2", load2m);

		query2Workload.put("q3", load1m);

		expected = new WorkloadSuite(query2Workload);

		assertEquals(expected.toString(), WorkloadSuite.fromConf(conf, CATEGORY_CEP).toString());
	}

	@Test
	public void testDefaultConf() {
		URL confDir = NexmarkGlobalConfigurationTest.class.getClassLoader().getResource("conf");
		assert confDir != null;
		Configuration conf = NexmarkGlobalConfiguration.loadConfiguration(confDir.getPath());

		Workload load = new Workload(10000000, 100000000, 1, 3, 46);

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
		query2Workload.put("q16", load);
		query2Workload.put("q17", load);
		query2Workload.put("q18", load);
		query2Workload.put("q19", load);
		query2Workload.put("q20", load);
		query2Workload.put("q21", load);
		query2Workload.put("q22", load);
		query2Workload.put("insert_kafka", new Workload(10000000, 0, 1, 3, 46));

		WorkloadSuite expected = new WorkloadSuite(query2Workload);

		assertEquals(expected.toString(), WorkloadSuite.fromConf(conf, CATEGORY_OA).toString());

		query2Workload = new HashMap<>();
		query2Workload.put("q0", load);
		query2Workload.put("q1", load);
		query2Workload.put("q2", load);
		query2Workload.put("q3", load);
		query2Workload.put("insert_kafka", new Workload(10000000, 0, 1, 3, 46));

		expected = new WorkloadSuite(query2Workload);

		assertEquals(expected.toString(), WorkloadSuite.fromConf(conf, CATEGORY_CEP).toString());
	}

	@Test
	public void testTPSValidation() {
		exception.expectMessage("You should configure 'nexmark.metric.monitor.duration'" +
				" in the TPS mode. Otherwise, the job will never end.");
		// TPS mode
		long eventsNum = 0L;
		new Workload(0L, eventsNum,0, 0, 0)
				.validateWorkload(FlinkNexmarkOptions.METRIC_MONITOR_DURATION.defaultValue());
	}

	@Test
	public void testEventsNumValidation() {
		exception.expectMessage("The configuration of 'nexmark.metric.monitor.duration'" +
				" is not supported in the events number mode.");
		// EventsNum mode
		long eventsNum = 100L;
		new Workload(0L, eventsNum,0, 0, 0)
				.validateWorkload(Duration.ofMillis(1000));
	}

	@Test
	public void testTPSMode() {
		// TPS mode
		long eventsNum = 0L;
		new Workload(0L, eventsNum,0, 0, 0)
				.validateWorkload(Duration.ofMillis(1000));
	}

	@Test
	public void testEventsNumMode() {
		// EventsNum mode
		long eventsNum = 100L;
		new Workload(0L, eventsNum,0, 0, 0)
				.validateWorkload(FlinkNexmarkOptions.METRIC_MONITOR_DURATION.defaultValue());
	}
}