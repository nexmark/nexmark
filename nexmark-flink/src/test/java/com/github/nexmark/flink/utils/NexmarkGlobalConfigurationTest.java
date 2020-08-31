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

package com.github.nexmark.flink.utils;

import org.apache.flink.configuration.Configuration;

import org.junit.Test;

import java.net.URL;

import static org.junit.Assert.assertEquals;

public class NexmarkGlobalConfigurationTest {

	@Test
	public void testLoadConfiguration() {
		URL confDir = NexmarkGlobalConfigurationTest.class.getClassLoader().getResource("conf");
		assert confDir != null;
		Configuration conf = NexmarkGlobalConfiguration.loadConfiguration(confDir.getPath());
		Configuration expected = new Configuration();
		expected.setString("nexmark.metric.monitor.delay", "3min");
		expected.setString("nexmark.metric.monitor.duration", "3min");
		expected.setString("flink.rest.address", "localhost");
		expected.setString("flink.rest.port", "8081");
		assertEquals(expected, conf);
	}
}