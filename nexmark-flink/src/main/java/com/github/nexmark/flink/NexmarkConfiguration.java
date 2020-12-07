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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import com.github.nexmark.flink.utils.NexmarkUtils;

import java.io.Serializable;
import java.util.Objects;

public class NexmarkConfiguration implements Serializable {

	/**
	 * Number of events to generate. If zero, generate as many as possible without overflowing
	 * internal counters etc.
	 */
	@JsonProperty
	public long numEvents = 0;

	/** Number of event generators to use. Each generates events in its own timeline. */
	@JsonProperty public int numEventGenerators = 1;

	/** Shape of event rate curve. */
	@JsonProperty public NexmarkUtils.RateShape rateShape = NexmarkUtils.RateShape.SQUARE;

	/** Initial overall event rate (in {@link #rateUnit}). */
	@JsonProperty public int firstEventRate = 10000;

	/** Next overall event rate (in {@link #rateUnit}). */
	@JsonProperty public int nextEventRate = 10000;

	/** Unit for rates. */
	@JsonProperty public NexmarkUtils.RateUnit rateUnit = NexmarkUtils.RateUnit.PER_SECOND;

	/** Overall period of rate shape, in seconds. */
	@JsonProperty public int ratePeriodSec = 600;

	/**
	 * Time in seconds to preload the subscription with data, at the initial input rate of the
	 * pipeline.
	 */
	@JsonProperty public int preloadSeconds = 0;

	/** Timeout for stream pipelines to stop in seconds. */
	@JsonProperty public int streamTimeout = 240;

	/**
	 * If true, and in streaming mode, generate events only when they are due according to their
	 * timestamp.
	 */
	@JsonProperty public boolean isRateLimited = false;

	/**
	 * If true, use wallclock time as event time. Otherwise, use a deterministic time in the past so
	 * that multiple runs will see exactly the same event streams and should thus have exactly the
	 * same results.
	 */
	@JsonProperty public boolean useWallclockEventTime = false;

	/**
	 * Person Proportion.
	 */
	@JsonProperty public int personProportion = 1;

	/**
	 * Auction Proportion.
	 */
	@JsonProperty public int auctionProportion = 3;

	/**
	 * Bid Proportion.
	 */
	@JsonProperty public int bidProportion = 46;

	/** Average idealized size of a 'new person' event, in bytes. */
	@JsonProperty public int avgPersonByteSize = 200;

	/** Average idealized size of a 'new auction' event, in bytes. */
	@JsonProperty public int avgAuctionByteSize = 500;

	/** Average idealized size of a 'bid' event, in bytes. */
	@JsonProperty public int avgBidByteSize = 100;

	/** Ratio of bids to 'hot' auctions compared to all other auctions. */
	@JsonProperty public int hotAuctionRatio = 2;

	/** Ratio of auctions for 'hot' sellers compared to all other people. */
	@JsonProperty public int hotSellersRatio = 4;

	/** Ratio of bids for 'hot' bidders compared to all other people. */
	@JsonProperty public int hotBiddersRatio = 4;

	/** Window size, in seconds, for queries 3, 5, 7 and 8. */
	@JsonProperty public long windowSizeSec = 10;

	/** Sliding window period, in seconds, for query 5. */
	@JsonProperty public long windowPeriodSec = 5;

	/** Number of seconds to hold back events according to their reported timestamp. */
	@JsonProperty public long watermarkHoldbackSec = 0;

	/** Average number of auction which should be inflight at any time, per generator. */
	@JsonProperty public int numInFlightAuctions = 100;

	/** Maximum number of people to consider as active for placing auctions or bids. */
	@JsonProperty public int numActivePeople = 1000;

	/** Length of occasional delay to impose on events (in seconds). */
	@JsonProperty public long occasionalDelaySec = 3;

	/** Probability that an event will be delayed by delayS. */
	@JsonProperty public double probDelayedEvent = 0.1;

	/**
	 * Number of events in out-of-order groups. 1 implies no out-of-order events. 1000 implies every
	 * 1000 events per generator are emitted in pseudo-random order.
	 */
	@JsonProperty public long outOfOrderGroupSize = 1;

	/** Return full description as a string. */
	@Override
	public String toString() {
		try {
			return NexmarkUtils.MAPPER.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		NexmarkConfiguration that = (NexmarkConfiguration) o;
		return numEvents == that.numEvents &&
			numEventGenerators == that.numEventGenerators &&
			firstEventRate == that.firstEventRate &&
			nextEventRate == that.nextEventRate &&
			ratePeriodSec == that.ratePeriodSec &&
			preloadSeconds == that.preloadSeconds &&
			streamTimeout == that.streamTimeout &&
			isRateLimited == that.isRateLimited &&
			useWallclockEventTime == that.useWallclockEventTime &&
			personProportion == that.personProportion &&
			auctionProportion == that.auctionProportion &&
			bidProportion == that.bidProportion &&
			avgPersonByteSize == that.avgPersonByteSize &&
			avgAuctionByteSize == that.avgAuctionByteSize &&
			avgBidByteSize == that.avgBidByteSize &&
			hotAuctionRatio == that.hotAuctionRatio &&
			hotSellersRatio == that.hotSellersRatio &&
			hotBiddersRatio == that.hotBiddersRatio &&
			windowSizeSec == that.windowSizeSec &&
			windowPeriodSec == that.windowPeriodSec &&
			watermarkHoldbackSec == that.watermarkHoldbackSec &&
			numInFlightAuctions == that.numInFlightAuctions &&
			numActivePeople == that.numActivePeople &&
			occasionalDelaySec == that.occasionalDelaySec &&
			Double.compare(that.probDelayedEvent, probDelayedEvent) == 0 &&
			outOfOrderGroupSize == that.outOfOrderGroupSize &&
			rateShape == that.rateShape &&
			rateUnit == that.rateUnit;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			numEvents,
			numEventGenerators,
			rateShape,
			firstEventRate,
			nextEventRate,
			rateUnit,
			ratePeriodSec,
			preloadSeconds,
			streamTimeout,
			isRateLimited,
			useWallclockEventTime,
			personProportion,
			auctionProportion,
			bidProportion,
			avgPersonByteSize,
			avgAuctionByteSize,
			avgBidByteSize,
			hotAuctionRatio,
			hotSellersRatio,
			hotBiddersRatio,
			windowSizeSec,
			windowPeriodSec,
			watermarkHoldbackSec,
			numInFlightAuctions,
			numActivePeople,
			occasionalDelaySec,
			probDelayedEvent,
			outOfOrderGroupSize);
	}
}
