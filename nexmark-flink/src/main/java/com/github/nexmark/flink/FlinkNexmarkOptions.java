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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/**
 * Options to control the nexmark-flink benchmark behaviors.
 */
public class FlinkNexmarkOptions {


	public static final ConfigOption<String> RUNNER_VERSION = ConfigOptions
			.key("nexmark.runner.version")
			.stringType()
			.defaultValue("v1")
			.withDescription("The version of the runner, v1 or v2.");

	public static final ConfigOption<Duration> METRIC_MONITOR_DELAY = ConfigOptions
		.key("nexmark.metric.monitor.delay")
		.durationType()
		.defaultValue(Duration.ofSeconds(10))
		.withDescription("When to monitor the metrics, default 10secs after job is started");

	public static final ConfigOption<Duration> METRIC_MONITOR_DURATION = ConfigOptions
		.key("nexmark.metric.monitor.duration")
		.durationType()
		.defaultValue(Duration.ofNanos(Long.MAX_VALUE))
		.withDescription("How long to monitor the metrics, default never end, " +
			"monitor until job is finished.");

	public static final ConfigOption<Duration> METRIC_MONITOR_INTERVAL = ConfigOptions
		.key("nexmark.metric.monitor.interval")
		.durationType()
		.defaultValue(Duration.ofSeconds(5))
		.withDescription("The interval to request the metrics.");


	public static final ConfigOption<String> METRIC_REPORTER_HOST = ConfigOptions
		.key("nexmark.metric.reporter.host")
		.stringType()
		.defaultValue("localhost")
		.withDescription("The metric reporter host that sender connects to.");

	public static final ConfigOption<Integer> METRIC_REPORTER_PORT = ConfigOptions
		.key("nexmark.metric.reporter.port")
		.intType()
		.defaultValue(9098)
		.withDescription("The metric reporter port that sender connects to.");

	public static final ConfigOption<String> METRIC_REPORTER_RECEIVING_HOST = ConfigOptions
		.key("nexmark.metric.reporter.receiving.host")
		.stringType()
		.noDefaultValue()
		.withDescription("The host for metric receiver to bind to. " +
			"If not set, falls back to nexmark.metric.reporter.host. " +
			"Useful in K8s where receiver binds to 0.0.0.0 but sender connects via service DNS.");

	public static final ConfigOption<Integer> METRIC_REPORTER_RECEIVING_PORT = ConfigOptions
		.key("nexmark.metric.reporter.receiving.port")
		.intType()
		.noDefaultValue()
		.withDescription("The port for metric receiver to bind to. " +
			"If not set, falls back to nexmark.metric.reporter.port.");

	public static final ConfigOption<String> FLINK_REST_ADDRESS = ConfigOptions
		.key("flink.rest.address")
		.stringType()
		.defaultValue("localhost")
		.withDescription("Flink REST address.");

	public static final ConfigOption<Integer> FLINK_REST_PORT = ConfigOptions
		.key("flink.rest.port")
		.intType()
		.defaultValue(8081)
		.withDescription("Flink REST port.");

}
