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

package com.github.nexmark.flink.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.github.nexmark.flink.generator.GeneratorConfig;
import com.github.nexmark.flink.utils.NexmarkUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.github.nexmark.flink.source.NexmarkTableSource.NEXMARK_SCHEMA;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link NexmarkTableSource} created by {@link NexmarkTableSourceFactory}.
 */
public class NexmarkTableSourceFactoryTest {

	@Test
	public void testCommonProperties() {
		Map<String, String> properties = getAllOptions();

		// validation for source
		DynamicTableSource actualSource = createTableSource(properties);
		GeneratorConfig config = new GeneratorConfig(
			new NexmarkConfiguration(),
			System.currentTimeMillis(),
			1,
			0,
			1
		);
		NexmarkTableSource expectedSource = new NexmarkTableSource(config);
		assertEquals(expectedSource, actualSource);
	}

	@Test
	public void testCustomProperties() {
		Map<String, String> properties = getAllOptions();
		properties.put("rate.shape", "SQUARE");
		properties.put("rate.period", "11 min");
		properties.put("rate.limited", "true");
		properties.put("first-event.rate", "99");
		properties.put("next-event.rate", "199");
		properties.put("person.avg-size", "1kb");
		properties.put("auction.avg-size", "5kb");
		properties.put("bid.avg-size", "8kb");
		properties.put("person.proportion", "30");
		properties.put("auction.proportion", "15");
		properties.put("bid.proportion", "5");
		properties.put("bid.hot-ratio.auctions", "3");
		properties.put("bid.hot-ratio.bidders", "5");
		properties.put("auction.hot-ratio.sellers", "8");
		properties.put("events.num", "100");

		DynamicTableSource actualSource = createTableSource(properties);
		NexmarkConfiguration nexmarkConf = new NexmarkConfiguration();
		nexmarkConf.rateShape = NexmarkUtils.RateShape.SQUARE;
		nexmarkConf.ratePeriodSec = 11 * 60;
		nexmarkConf.isRateLimited = true;
		nexmarkConf.firstEventRate = 99;
		nexmarkConf.nextEventRate = 199;
		nexmarkConf.avgPersonByteSize = 1024;
		nexmarkConf.avgAuctionByteSize = 5 * 1024;
		nexmarkConf.avgBidByteSize = 8 * 1024;
		nexmarkConf.personProportion = 30;
		nexmarkConf.auctionProportion = 15;
		nexmarkConf.bidProportion = 5;
		nexmarkConf.hotAuctionRatio = 3;
		nexmarkConf.hotBiddersRatio = 5;
		nexmarkConf.hotSellersRatio = 8;
		nexmarkConf.numEvents = 100;

		GeneratorConfig config = new GeneratorConfig(
			nexmarkConf,
			System.currentTimeMillis(),
			1,
			nexmarkConf.numEvents,
			1
		);
		NexmarkTableSource expectedSource = new NexmarkTableSource(config);
		assertEquals(expectedSource, actualSource);
	}

	private Map<String, String> getAllOptions() {
		Map<String, String> options = new HashMap<>();
		options.put("connector", "nexmark");
		return options;
	}

	private static DynamicTableSource createTableSource(Map<String, String> options) {
		return FactoryUtil.createTableSource(
			null,
			ObjectIdentifier.of("default", "default", "t1"),
			new ResolvedCatalogTable(
					CatalogTable.of(
							NEXMARK_SCHEMA.toSchema(),
							"mock source",
							new ArrayList<>(),
							options),
					ResolvedSchema.physical(
							NEXMARK_SCHEMA.getFieldNames(), NEXMARK_SCHEMA.getFieldDataTypes())),
			new Configuration(),
			NexmarkTableSourceFactoryTest.class.getClassLoader(),
			false);
	}
}
