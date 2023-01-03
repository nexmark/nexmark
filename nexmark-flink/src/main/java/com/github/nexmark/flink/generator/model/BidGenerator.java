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

import org.apache.flink.table.utils.ThreadLocalCache;

import com.github.nexmark.flink.generator.GeneratorConfig;
import com.github.nexmark.flink.model.Bid;

import java.util.concurrent.ConcurrentHashMap;
import java.time.Instant;
import java.util.Random;

import static com.github.nexmark.flink.generator.model.StringsGenerator.nextString;

/** Generates bids. */
public class BidGenerator {

  /**
   * Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are 1 over these
   * values.
   */
  private static final int HOT_AUCTION_RATIO = 100;

  private static final int HOT_BIDDER_RATIO = 100;

  private static final int HOT_CHANNELS_RATIO = 2;

  private static final int CHANNELS_NUMBER = 10_000;

  private static final String[] HOT_CHANNELS = new String[] {"Google", "Facebook", "Baidu", "Apple"};
  private static final String[] HOT_URLS = new String[] {getBaseUrl(), getBaseUrl(), getBaseUrl(), getBaseUrl()};

  private static final ConcurrentHashMap<Integer, Tuple2<String, String>> CHANNEL_URL_CACHE = new ConcurrentHashMap<Integer, Tuple2<String, String>>(CHANNELS_NUMBER);
  
  private static class Tuple2<T1,T2> {
    public final T1 f0;
    public final T2 f1;
    public Tuple2(T1 f0, T2 f1){
      this.f0 = f0;
      this.f1 = f1;
    }
  }

  /** Generate and return a random bid with next available id. */
  public static Bid nextBid(long eventId, Random random, long timestamp, GeneratorConfig config) {

    long auction;
    // Here P(bid will be for a hot auction) = 1 - 1/hotAuctionRatio.
    if (random.nextInt(config.getHotAuctionRatio()) > 0) {
      // Choose the first auction in the batch of last HOT_AUCTION_RATIO auctions.
      auction = (AuctionGenerator.lastBase0AuctionId(config, eventId) / HOT_AUCTION_RATIO) * HOT_AUCTION_RATIO;
    } else {
      auction = AuctionGenerator.nextBase0AuctionId(eventId, random, config);
    }
    auction += GeneratorConfig.FIRST_AUCTION_ID;

    long bidder;
    // Here P(bid will be by a hot bidder) = 1 - 1/hotBiddersRatio
    if (random.nextInt(config.getHotBiddersRatio()) > 0) {
      // Choose the second person (so hot bidders and hot sellers don't collide) in the batch of
      // last HOT_BIDDER_RATIO people.
      bidder = (PersonGenerator.lastBase0PersonId(config, eventId) / HOT_BIDDER_RATIO) * HOT_BIDDER_RATIO + 1;
    } else {
      bidder = PersonGenerator.nextBase0PersonId(eventId, random, config);
    }
    bidder += GeneratorConfig.FIRST_PERSON_ID;

    long price = PriceGenerator.nextPrice(random);

    String channel;
    String url;
    if (random.nextInt(HOT_CHANNELS_RATIO) > 0) {
      int i = random.nextInt(HOT_CHANNELS.length);
      channel = HOT_CHANNELS[i];
      url = HOT_URLS[i];
    } else {
      Tuple2<String, String> channelAndUrl = getNextChannelAndurl(random.nextInt(CHANNELS_NUMBER));
      channel = channelAndUrl.f0;
      url = channelAndUrl.f1;
    }

    bidder += GeneratorConfig.FIRST_PERSON_ID;

    int currentSize = 8 + 8 + 8 + 8;
    String extra = StringsGenerator.nextExtra(random, currentSize, config.getAvgBidByteSize());
    return new Bid(auction, bidder, price, channel, url, Instant.ofEpochMilli(timestamp), extra);
  }

  private static String getBaseUrl() {
    Random random = new Random();
    return "https://www.nexmark.com/" +
            nextString(random, 5, '_') + '/' +
            nextString(random, 5, '_') + '/' +
            nextString(random, 5, '_') + '/' +
            "item.htm?query=1";
  }

  private static Tuple2<String, String> getNextChannelAndurl(int channelNumber){
    Tuple2<String, String> previousValue = CHANNEL_URL_CACHE.get(channelNumber);
    if(previousValue == null){
      String url = getBaseUrl();
      if (new Random().nextInt(10) > 0) {
        url = url + "&channel_id=" + Math.abs(Integer.reverse(channelNumber));
      }
      
      CHANNEL_URL_CACHE.putIfAbsent(channelNumber, new Tuple2<String, String>("channel-" + channelNumber, url));
    }

    return CHANNEL_URL_CACHE.get(channelNumber);
  }
}
