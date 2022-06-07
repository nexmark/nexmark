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
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.Before;
import org.junit.Test;

public class NexmarkTableSourceITCase {

	private StreamTableEnvironment tEnv;

	@Before
	public void before() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
			"        channel  VARCHAR,\n" +
			"        url  VARCHAR,\n" +
			"        dateTime  TIMESTAMP(3),\n" +
			"        extra  VARCHAR>,\n" +
			"    dateTime AS\n" +
			"        CASE\n" +
			"            WHEN event_type = 0 THEN person.dateTime\n" +
			"            WHEN event_type = 1 THEN auction.dateTime\n" +
			"            ELSE bid.dateTime\n" +
			"        END,\n" +
			"    WATERMARK FOR dateTime AS dateTime - INTERVAL '4' SECOND" +
			") WITH (\n" +
			"    'connector' = 'nexmark',\n" +
			"    'events.num' = '500'\n" +
			")");
		tEnv.executeSql("CREATE VIEW person AS\n" +
			"SELECT  person.id,\n" +
				"    person.name,\n" +
				"    person.emailAddress,\n" +
				"    person.creditCard,\n" +
				"    person.city,\n" +
				"    person.state,\n" +
				"    dateTime,\n" +
				"    person.extra FROM nexmark AS t WHERE event_type = 0");
		tEnv.executeSql("CREATE VIEW auction AS\n" +
			"SELECT  auction.id,\n" +
				"    auction.itemName,\n" +
				"    auction.description,\n" +
				"    auction.initialBid,\n" +
				"    auction.reserve,\n" +
				"    dateTime,\n" +
				"    auction.expires,\n" +
				"    auction.seller,\n" +
				"    auction.category,\n" +
				"    auction.extra FROM nexmark AS t WHERE event_type = 1");
		tEnv.executeSql("CREATE VIEW bid AS\n" +
			"SELECT  bid.auction,\n" +
				"    bid.bidder,\n" +
				"    bid.price,\n" +
				"    bid.channel,\n" +
				"    bid.url,\n" +
				"    dateTime,\n" +
				"    bid.extra FROM nexmark AS t WHERE event_type = 2");
	}

	@Test
	public void testAllEvents() {
		print(tEnv.executeSql("SELECT * FROM nexmark"));
	}

	@Test
	public void testPersonSource() {
		print(tEnv.executeSql("SELECT * FROM person"));
	}

	@Test
	public void testAuctionSource() {
		print(tEnv.executeSql("SELECT * FROM auction"));
	}

	@Test
	public void testBidSource() {
		print(tEnv.executeSql("SELECT * FROM bid"));
	}

	@Test
	public void q16() {
		print(tEnv.executeSql("SELECT\n" +
				"    channel,\n" +
				"    DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,\n" +
				"    max(DATE_FORMAT(dateTime, 'HH:mm')) as `minute`,\n" +
				"    count(*) AS total_bids,\n" +
				"    count(*) filter (where price < 10000) AS rank1_bids,\n" +
				"        count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,\n" +
				"        count(*) filter (where price >= 1000000) AS rank3_bids,\n" +
				"        count(distinct bidder) AS total_bidders,\n" +
				"    count(distinct bidder) filter (where price < 10000) AS rank1_bidders,\n" +
				"        count(distinct bidder) filter (where price >= 10000 and price < 1000000) AS rank2_bidders,\n" +
				"        count(distinct bidder) filter (where price >= 1000000) AS rank3_bidders,\n" +
				"        count(distinct auction) AS total_auctions,\n" +
				"    count(distinct auction) filter (where price < 10000) AS rank1_auctions,\n" +
				"        count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,\n" +
				"        count(distinct auction) filter (where price >= 1000000) AS rank3_auctions\n" +
				"FROM bid\n" +
				"GROUP BY channel, DATE_FORMAT(dateTime, 'yyyy-MM-dd')"));
	}

	@Test
	public void q17() {
		print(tEnv.executeSql("SELECT\n" +
				"     auction,\n" +
				"     DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,\n" +
				"     count(*) AS total_bids,\n" +
				"     count(*) filter (where price < 10000) AS rank1_bids,\n" +
				"     count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,\n" +
				"     count(*) filter (where price >= 1000000) AS rank3_bids,\n" +
				"     min(price) AS min_price,\n" +
				"     max(price) AS max_price,\n" +
				"     avg(price) AS avg_price,\n" +
				"     sum(price) AS sum_price\n" +
				"FROM bid\n" +
				"GROUP BY auction, DATE_FORMAT(dateTime, 'yyyy-MM-dd')"));
	}

	@Test
	public void q18() {
		print(tEnv.executeSql("SELECT auction, bidder, price, channel, url, dateTime, extra\n" +
				" FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY dateTime DESC) AS rank_number\n" +
				"       FROM bid)\n" +
				" WHERE rank_number <= 1"));
	}

	@Test
	public void q19() {
		print(tEnv.executeSql("SELECT * FROM\n" +
				"(SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid)\n" +
				"WHERE rank_number <= 10"));
	}

	@Test
	public void q20() {
		removeRowTime();
		print(tEnv.executeSql("SELECT\n" +
				"    auction, bidder, price, channel, url, B.dateTime, B.extra,\n" +
				"    itemName, description, initialBid, reserve, A.dateTime, expires, seller, category, A.extra\n" +
				"FROM\n" +
				"    bid AS B INNER JOIN auction AS A on B.auction = A.id\n" +
				"WHERE A.category = 10"));
	}

	@Test
	public void q21() {
		print(tEnv.executeSql("SELECT\n" +
				"    auction, bidder, price, channel,\n" +
				"    CASE\n" +
				"        WHEN lower(channel) = 'apple' THEN '0'\n" +
				"        WHEN lower(channel) = 'google' THEN '1'\n" +
				"        WHEN lower(channel) = 'facebook' THEN '2'\n" +
				"        WHEN lower(channel) = 'baidu' THEN '3'\n" +
				"        ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2)\n" +
				"        END\n" +
				"    AS channel_id FROM bid\n" +
				"    where REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) is not null or\n" +
				"          lower(channel) in ('apple', 'google', 'facebook', 'baidu')"));
	}

	@Test
	public void q22() {
		print(tEnv.executeSql("SELECT\n" +
				"    auction, bidder, price, channel,\n" +
				"    SPLIT_INDEX(url, '/', 3) as dir1,\n" +
				"    SPLIT_INDEX(url, '/', 4) as dir2,\n" +
				"    SPLIT_INDEX(url, '/', 5) as dir3 FROM bid"));
	}

	@Test
	public void cepQ0() {
		print(
				tEnv.executeSql(
						"SELECT\n"
								+ "    auction, bidder, start_tstamp, bottom_tstamp, end_tstamp\n"
								+ "FROM bid\n"
								+ "MATCH_RECOGNIZE (\n"
								+ "    PARTITION BY auction, bidder\n"
								+ "    ORDER BY dateTime\n"
								+ "    MEASURES\n"
								+ "        START_ROW.dateTime AS start_tstamp,\n"
								+ "        LAST(PRICE_DOWN.dateTime) AS bottom_tstamp,\n"
								+ "        LAST(PRICE_UP.dateTime) AS end_tstamp\n"
								+ "    ONE ROW PER MATCH\n"
								+ "    AFTER MATCH SKIP TO LAST PRICE_UP\n"
								+ "    PATTERN (START_ROW PRICE_DOWN+ PRICE_UP)\n"
								+ "    DEFINE\n"
								+ "        PRICE_DOWN AS\n"
								+ "            (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR\n"
								+ "                PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1),\n"
								+ "        PRICE_UP AS\n"
								+ "            PRICE_UP.price > LAST(PRICE_DOWN.price, 1)\n"
								+ ")"));
	}

	@Test
	public void cepQ1() {
		print(
				tEnv.executeSql(
						"SELECT\n"
								+ "    auction, bidder, start_tstamp, end_tstamp, avg_price\n"
								+ "FROM bid\n"
								+ "MATCH_RECOGNIZE (\n"
								+ "    PARTITION BY auction, bidder\n"
								+ "    ORDER BY dateTime\n"
								+ "    MEASURES\n"
								+ "        FIRST(A.dateTime) AS start_tstamp,\n"
								+ "        LAST(A.dateTime) AS end_tstamp,\n"
								+ "        AVG(A.price) AS avg_price\n"
								+ "    ONE ROW PER MATCH\n"
								+ "    AFTER MATCH SKIP PAST LAST ROW\n"
								+ "    PATTERN (A+ B)\n"
								+ "    DEFINE\n"
								+ "        A AS AVG(A.price) < 10000\n"
								+ ")"));
	}

	@Test
	public void cepQ2() {
		print(
				tEnv.executeSql(
						"SELECT\n"
								+ "    auction, bidder, start_tstamp, end_tstamp, avg_price\n"
								+ "FROM bid\n"
								+ "MATCH_RECOGNIZE (\n"
								+ "    PARTITION BY auction, bidder\n"
								+ "    ORDER BY dateTime\n"
								+ "    MEASURES\n"
								+ "        FIRST(A.dateTime) AS start_tstamp,\n"
								+ "        LAST(A.dateTime) AS end_tstamp,\n"
								+ "        AVG(A.price) AS avg_price\n"
								+ "    ONE ROW PER MATCH\n"
								+ "    AFTER MATCH SKIP TO NEXT ROW\n"
								+ "    PATTERN (A+ B)\n"
								+ "    DEFINE\n"
								+ "        A AS AVG(A.price) < 10000\n"
								+ ")"));
	}

	@Test
	public void cepQ3() {
		print(
				tEnv.executeSql(
						"SELECT\n"
								+ "    auction, bidder, drop_time, drop_diff\n"
								+ "FROM bid\n"
								+ "MATCH_RECOGNIZE(\n"
								+ "    PARTITION BY auction, bidder\n"
								+ "    ORDER BY dateTime\n"
								+ "    MEASURES\n"
								+ "        C.dateTime AS drop_time,\n"
								+ "        A.price - C.price AS drop_diff\n"
								+ "    ONE ROW PER MATCH\n"
								+ "    AFTER MATCH SKIP PAST LAST ROW\n"
								+ "    PATTERN (A B* C) WITHIN INTERVAL '5' SECOND\n"
								+ "    DEFINE\n"
								+ "        B AS B.price > A.price - 50,\n"
								+ "        C AS C.price < A.price - 50\n"
								+ ")"));
	}

	private void print(TableResult result) {
		// TableResult.print will truncate string value
		try (CloseableIterator<Row> iter = result.collect()) {
			while (iter.hasNext()) {
				System.out.println(iter.next());
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void removeRowTime() {
		tEnv.executeSql("DROP VIEW IF EXISTS person");
		tEnv.executeSql("DROP VIEW IF EXISTS auction");
		tEnv.executeSql("DROP VIEW IF EXISTS bid");
		tEnv.executeSql("CREATE VIEW person AS SELECT person.* FROM nexmark WHERE event_type = 0");
		tEnv.executeSql("CREATE VIEW auction AS SELECT auction.* FROM nexmark WHERE event_type = 1");
		tEnv.executeSql("CREATE VIEW bid AS SELECT bid.* FROM nexmark WHERE event_type = 2");
	}
}
