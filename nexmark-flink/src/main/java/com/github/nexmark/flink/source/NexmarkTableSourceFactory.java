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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.github.nexmark.flink.generator.GeneratorConfig;
import com.github.nexmark.flink.NexmarkConfiguration;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class NexmarkTableSourceFactory implements DynamicTableSourceFactory {

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();
		helper.validate();

		// for compatibility reason of "context.getCatalogTable()", do not validate schema.
		// validateSchema(TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema()));

		int parallelism = context.getConfiguration().get(CoreOptions.DEFAULT_PARALLELISM);
		NexmarkConfiguration nexmarkConf = NexmarkSourceOptions.convertToNexmarkConfiguration(config);
		nexmarkConf.numEventGenerators = parallelism;
		GeneratorConfig generatorConfig = new GeneratorConfig(
			nexmarkConf,
			System.currentTimeMillis(),
			1,
			nexmarkConf.numEvents,
			1);

		return new NexmarkTableSource(generatorConfig);
	}

	private void validateSchema(TableSchema schema) {
		if (!schema.equals(NexmarkTableSource.NEXMARK_SCHEMA)) {
			throw new IllegalArgumentException(
				String.format("The nexmark source table must be in the schema of \n%s\n. However, It is \n%s\n",
					NexmarkTableSource.NEXMARK_SCHEMA,
					schema));
		}
	}

	@Override
	public String factoryIdentifier() {
		return "nexmark";
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Collections.emptySet();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> sets = new HashSet<>();
		sets.add(NexmarkSourceOptions.RATE_SHAPE);
		sets.add(NexmarkSourceOptions.RATE_PERIOD);
		sets.add(NexmarkSourceOptions.RATE_LIMITED);
		sets.add(NexmarkSourceOptions.FIRST_EVENT_RATE);
		sets.add(NexmarkSourceOptions.NEXT_EVENT_RATE);
		sets.add(NexmarkSourceOptions.PERSON_AVG_SIZE);
		sets.add(NexmarkSourceOptions.AUCTION_AVG_SIZE);
		sets.add(NexmarkSourceOptions.BID_AVG_SIZE);
		sets.add(NexmarkSourceOptions.PERSON_PROPORTION);
		sets.add(NexmarkSourceOptions.AUCTION_PROPORTION);
		sets.add(NexmarkSourceOptions.BID_PROPORTION);
		sets.add(NexmarkSourceOptions.BID_HOT_RATIO_AUCTIONS);
		sets.add(NexmarkSourceOptions.BID_HOT_RATIO_BIDDERS);
		sets.add(NexmarkSourceOptions.AUCTION_HOT_RATIO_SELLERS);
		sets.add(NexmarkSourceOptions.EVENTS_NUM);
		return sets;
	}
}
