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

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Before;
import org.junit.Test;

public class NexmarkTableSourceITCase {

	private StreamExecutionEnvironment env;
	private StreamTableEnvironment tEnv;

	@Before
	public void before() {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		tEnv = StreamTableEnvironment.create(env);
		env.setParallelism(4);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointInterval(1000L);

		tEnv.executeSql("CREATE TABLE nexmark (\n" +
			"    event_type int,\n" +
			"    person ROW<\n" +
			"        id  BIGINT,\n" +
			"        name  VARCHAR,\n" +
			"        emailAddress  VARCHAR,\n" +
			"        creditCard  VARCHAR,\n" +
			"        city  VARCHAR,\n" +
			"        state  VARCHAR,\n" +
			"        dateTime TIMESTAMP(3),\n" +
			"        extra  VARCHAR>,\n" +
			"    auction ROW<\n" +
			"        id  BIGINT,\n" +
			"        itemName  VARCHAR,\n" +
			"        description  VARCHAR,\n" +
			"        initialBid  BIGINT,\n" +
			"        reserve  BIGINT,\n" +
			"        dateTime  TIMESTAMP(3),\n" +
			"        expires  TIMESTAMP(3),\n" +
			"        seller  BIGINT,\n" +
			"        category  BIGINT,\n" +
			"        extra  VARCHAR>,\n" +
			"    bid ROW<\n" +
			"        auction  BIGINT,\n" +
			"        bidder  BIGINT,\n" +
			"        price  BIGINT,\n" +
			"        dateTime  TIMESTAMP(3),\n" +
			"        extra  VARCHAR>\n" +
			") WITH (\n" +
			"    'connector' = 'nexmark',\n" +
			"    'events.num' = '100'\n" +
			")");
		tEnv.executeSql("CREATE VIEW person AS\n" +
			"SELECT t.person.* FROM nexmark AS t WHERE event_type = 0");
		tEnv.executeSql("CREATE VIEW auction AS\n" +
			"SELECT t.auction.* FROM nexmark AS t WHERE event_type = 1");
		tEnv.executeSql("CREATE VIEW bid AS\n" +
			"SELECT t.bid.* FROM nexmark AS t WHERE event_type = 2");
	}

	@Test
	public void testAllEvents() {
		tEnv.executeSql("SELECT * FROM nexmark").print();
	}

	@Test
	public void testPersonSource() {
		tEnv.executeSql("SELECT * FROM person").print();
	}


	@Test
	public void testAuctionSource() {
		tEnv.executeSql("SELECT * FROM auction").print();
	}

	@Test
	public void testBidSource() {
		tEnv.executeSql("SELECT * FROM bid").print();
	}

}
