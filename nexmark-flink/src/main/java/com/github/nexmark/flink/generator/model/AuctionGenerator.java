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
package com.github.nexmark.flink.generator.model;

import com.github.nexmark.flink.model.Auction;
import com.github.nexmark.flink.generator.GeneratorConfig;

import java.time.Instant;
import java.util.SplittableRandom;

/** AuctionGenerator. */
public class AuctionGenerator {
  /**
   * Keep the number of categories small so the example queries will find results even with a small
   * batch of events.
   */
  private static final int NUM_CATEGORIES = 5;

  /** Number of yet-to-be-created people and auction ids allowed. */
  private static final int AUCTION_ID_LEAD = 10;

  /**
   * Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are 1 over these
   * values.
   */
  private static final int HOT_SELLER_RATIO = 100;

  /** Generate and return a random auction with next available id. */
  public static Auction nextAuction(
          long eventsCountSoFar, long eventId, SplittableRandom random, long timestamp, GeneratorConfig config) {

    long id = lastBase0AuctionId(config, eventId) + GeneratorConfig.FIRST_AUCTION_ID;

    long seller;
    // Here P(auction will be for a hot seller) = 1 - 1/hotSellersRatio.
    if (random.nextInt(config.getHotSellersRatio()) > 0) {
      // Choose the first person in the batch of last HOT_SELLER_RATIO people.
      seller = (PersonGenerator.lastBase0PersonId(config, eventId) / HOT_SELLER_RATIO) * HOT_SELLER_RATIO;
    } else {
      seller = PersonGenerator.nextBase0PersonId(eventId, random, config);
    }
    seller += GeneratorConfig.FIRST_PERSON_ID;

    long category = GeneratorConfig.FIRST_CATEGORY_ID + random.nextInt(NUM_CATEGORIES);
    long initialBid = PriceGenerator.nextPrice(random);
    long expires = timestamp + nextAuctionLengthMs(eventsCountSoFar, random, timestamp, config);
    String name = StringsGenerator.nextString(random, 20);
    String desc = StringsGenerator.nextString(random, 100);
    long reserve = initialBid + PriceGenerator.nextPrice(random);
    int currentSize = 8 + name.length() + desc.length() + 8 + 8 + 8 + 8 + 8;
    String extra = StringsGenerator.nextExtra(random, currentSize, config.getAvgAuctionByteSize());
    return new Auction(
        id,
        name,
        desc,
        initialBid,
        reserve,
        Instant.ofEpochMilli(timestamp),
        Instant.ofEpochMilli(expires),
        seller,
        category,
        extra);
  }

  /**
   * Return the last valid auction id (ignoring FIRST_AUCTION_ID). Will be the current auction id if
   * due to generate an auction.
   */
  public static long lastBase0AuctionId(GeneratorConfig config, long eventId) {
    long epoch = eventId / config.totalProportion;
    long offset = eventId % config.totalProportion;
    if (offset < config.personProportion) {
      // About to generate a person.
      // Go back to the last auction in the last epoch.
      epoch--;
      offset = config.auctionProportion - 1;
    } else if (offset >= config.personProportion + config.auctionProportion) {
      // About to generate a bid.
      // Go back to the last auction generated in this epoch.
      offset = config.auctionProportion - 1;
    } else {
      // About to generate an auction.
      offset -= config.personProportion;
    }
    return epoch * config.auctionProportion + offset;
  }

  /** Return a random auction id (base 0). */
  public static long nextBase0AuctionId(long nextEventId, SplittableRandom random, GeneratorConfig config) {

    // Choose a random auction for any of those which are likely to still be in flight,
    // plus a few 'leads'.
    // Note that ideally we'd track non-expired auctions exactly, but that state
    // is difficult to split.
    long minAuction =
        Math.max(lastBase0AuctionId(config, nextEventId) - config.getNumInFlightAuctions(), 0);
    long maxAuction = lastBase0AuctionId(config, nextEventId);
    return minAuction + LongGenerator.nextLong(random, maxAuction - minAuction + 1 + AUCTION_ID_LEAD);
  }

  /** Return a random time delay, in milliseconds, for length of auctions. */
  private static long nextAuctionLengthMs(
      long eventsCountSoFar, SplittableRandom random, long timestamp, GeneratorConfig config) {

    // What's our current event number?
    long currentEventNumber = config.nextAdjustedEventNumber(eventsCountSoFar);
    // How many events till we've generated numInFlightAuctions?
    long numEventsForAuctions =
        ((long) config.getNumInFlightAuctions() * config.totalProportion)
            / config.auctionProportion;
    // When will the auction numInFlightAuctions beyond now be generated?
    long futureAuction = config.timestampForEvent(currentEventNumber + numEventsForAuctions);
    // System.out.printf("*** auction will be for %dms (%d events ahead) ***\n",
    //     futureAuction - timestamp, numEventsForAuctions);
    // Choose a length with average horizonMs.
    long horizonMs = futureAuction - timestamp;
    return 1L + LongGenerator.nextLong(random, Math.max(horizonMs * 2, 1L));
  }
}
