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

import com.github.nexmark.flink.source.NexmarkSourceOptions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class WorkloadSuite {

	private static final String WORKLOAD_SUITE_CONF_PREFIX = "nexmark.workload.suite.";
	private static final String TPS_CONF_SUFFIX = ".tps";

	private final Map<String, Workload> query2Workload;

	WorkloadSuite(Map<String, Workload> query2Workload) {
		this.query2Workload = query2Workload;
	}

	public Workload getQueryWorkload(String queryName) {
		return query2Workload.get(queryName);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		WorkloadSuite that = (WorkloadSuite) o;
		return Objects.equals(query2Workload, that.query2Workload);
	}

	@Override
	public int hashCode() {
		return Objects.hash(query2Workload);
	}

	@Override
	public String toString() {
		return "WorkloadSuite{" +
			"query2Workload=" + query2Workload +
			'}';
	}

	public static WorkloadSuite fromConf(Configuration nexmarkConf) {
		Map<String, String> confMap = nexmarkConf.toMap();
		Set<String> suites = new HashSet<>();
		confMap.keySet().forEach(k -> {
			if (k.startsWith(WORKLOAD_SUITE_CONF_PREFIX) && k.endsWith(TPS_CONF_SUFFIX)) {
				String suiteName = k.substring(WORKLOAD_SUITE_CONF_PREFIX.length(), k.length() - TPS_CONF_SUFFIX.length());
				suites.add(suiteName);
			}
		});

		Map<String, Workload> query2Workload = new HashMap<>();
		for (String suiteName : suites) {
			String tpsKey = WORKLOAD_SUITE_CONF_PREFIX + suiteName + TPS_CONF_SUFFIX;
			long tps = Long.parseLong(confMap.get(tpsKey));

			int personProportion = NexmarkSourceOptions.PERSON_PROPORTION.defaultValue();
			int auctionProportion = NexmarkSourceOptions.AUCTION_PROPORTION.defaultValue();
			int bidProportion = NexmarkSourceOptions.BID_PROPORTION.defaultValue();
			String percentageKey = WORKLOAD_SUITE_CONF_PREFIX + suiteName + ".percentage";
			if (confMap.containsKey(percentageKey)) {
				String percentage = removeQuotes(confMap.get(percentageKey));
				String[] percentageArray = percentage.split(",");
				for (String str : percentageArray) {
					String part = str.trim();
					if (part.startsWith("bid:")) {
						bidProportion = Integer.parseInt(part.substring("bid:".length()));
					} else if (part.startsWith("auction:")) {
						auctionProportion = Integer.parseInt(part.substring("auction:".length()));
					} else if (part.startsWith("person:")) {
						personProportion = Integer.parseInt(part.substring("person:".length()));
					} else {
						throw new IllegalArgumentException("Unable to parse suite percentage: " + percentage);
					}
				}
			}
			Workload load = new Workload(tps, personProportion, auctionProportion, bidProportion);

			String queriesKey = WORKLOAD_SUITE_CONF_PREFIX + suiteName + ".queries";
			List<String> queries = new ArrayList<>();
			if (confMap.containsKey(queriesKey)) {
				String queriesString = removeQuotes(confMap.get(queriesKey));
				for (String q : queriesString.split(",")) {
					queries.add(q.trim());
				}
			}

			for (String q : queries) {
				Workload old = query2Workload.put(q, load);
				if (old != null) {
					throw new IllegalArgumentException(
						String.format("Query %s is defined in multiple suites.", q));
				}
			}
		}

		return new WorkloadSuite(query2Workload);
	}

	private static String removeQuotes(String str) {
		String result = str;
		if (result.startsWith("\"")) {
			result = result.substring(1);
		}
		if (result.endsWith("\"")) {
			result = result.substring(0, result.length() - 1);
		}
		return result;
	}

}
