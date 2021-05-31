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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;

import com.github.nexmark.flink.generator.GeneratorConfig;

import java.util.Objects;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;

/**
 * Table source for Nexmark.
 */
public class NexmarkTableSource implements ScanTableSource {

	public static final TableSchema NEXMARK_SCHEMA = TableSchema.builder()
		.field("event_type", INT())
		.field("person", ROW(
			FIELD("id", BIGINT()),
			FIELD("name", STRING()),
			FIELD("emailAddress", STRING()),
			FIELD("creditCard", STRING()),
			FIELD("city", STRING()),
			FIELD("state", STRING()),
			FIELD("dateTime", TIMESTAMP(3)),
			FIELD("extra", STRING())))
		.field("auction", ROW(
			FIELD("id", BIGINT()),
			FIELD("itemName", STRING()),
			FIELD("description", STRING()),
			FIELD("initialBid", BIGINT()),
			FIELD("reserve", BIGINT()),
			FIELD("dateTime", TIMESTAMP(3)),
			FIELD("expires", TIMESTAMP(3)),
			FIELD("seller", BIGINT()),
			FIELD("category", BIGINT()),
			FIELD("extra", STRING())))
		.field("bid", ROW(
			FIELD("auction", BIGINT()),
			FIELD("bidder", BIGINT()),
			FIELD("price", BIGINT()),
			FIELD("channel", STRING()),
			FIELD("url", STRING()),
			FIELD("dateTime", TIMESTAMP(3)),
			FIELD("extra", STRING())))
		.build();

	private final GeneratorConfig config;

	public NexmarkTableSource(GeneratorConfig config) {
		this.config = config;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@SuppressWarnings("unchecked")
	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
		// No type for be compatible for Flink 1.11 and 1.13
		TypeInformation outputType = scanContext
			.createTypeInformation(NEXMARK_SCHEMA.toPhysicalRowDataType());
		NexmarkSourceFunction<RowData> sourceFunction = new NexmarkSourceFunction<>(
			config,
			new RowDataEventDeserializer(),
			outputType);
		return SourceFunctionProvider.of(sourceFunction, false);
	}

	@Override
	public DynamicTableSource copy() {
		return new NexmarkTableSource(config);
	}

	@Override
	public String asSummaryString() {
		return "Nexmark Source";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		NexmarkTableSource that = (NexmarkTableSource) o;
		return Objects.equals(config, that.config);
	}

	@Override
	public int hashCode() {
		return Objects.hash(config);
	}

	@Override
	public String toString() {
		return "NexmarkTableSource{" +
			"config=" + config +
			'}';
	}
}
