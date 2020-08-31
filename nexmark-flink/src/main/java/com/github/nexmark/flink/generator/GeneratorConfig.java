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
package com.github.nexmark.flink.generator;

import com.github.nexmark.flink.model.Event;
import com.github.nexmark.flink.NexmarkConfiguration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Parameters controlling how {@link NexmarkGenerator} synthesizes {@link Event} elements. */
public class GeneratorConfig implements Serializable {

  /**
   * We start the ids at specific values to help ensure the queries find a match even on small
   * synthesized dataset sizes.
   */
  public static final long FIRST_AUCTION_ID = 1000L;

  public static final long FIRST_PERSON_ID = 1000L;
  public static final long FIRST_CATEGORY_ID = 10L;

  /** Proportions of people/auctions/bids to synthesize. */
  public final int personProportion;
  public final int auctionProportion;
  public final int bidProportion;
  public final int totalProportion;

  /** Environment options. */
  private final NexmarkConfiguration configuration;

  /**
   * Delay between events, in microseconds. If the array has more than one entry then the rate is
   * changed every {@link #stepLengthSec}, and wraps around.
   */
  private final double[] interEventDelayUs;

  /** Delay before changing the current inter-event delay. */
  private final long stepLengthSec;

  /** Time for first event (ms since epoch). */
  public final long baseTime;

  /**
   * Event id of first event to be generated. Event ids are unique over all generators, and are used
   * as a seed to generate each event's data.
   */
  public final long firstEventId;

  /** Maximum number of events to generate. */
  public final long maxEvents;

  /**
   * First event number. Generators running in parallel time may share the same event number, and
   * the event number is used to determine the event timestamp.
   */
  public final long firstEventNumber;

  /**
   * True period of epoch in milliseconds. Derived from above. (Ie time to run through cycle for all
   * interEventDelayUs entries).
   */
  private final long epochPeriodMs;

  /**
   * Number of events per epoch. Derived from above. (Ie number of events to run through cycle for
   * all interEventDelayUs entries).
   */
  private final long eventsPerEpoch;

  public GeneratorConfig(
      NexmarkConfiguration configuration,
      long baseTime,
      long firstEventId,
      long maxEventsOrZero,
      long firstEventNumber) {

    this.auctionProportion = configuration.auctionProportion;
    this.personProportion = configuration.personProportion;
    this.bidProportion = configuration.bidProportion;
    this.totalProportion = this.auctionProportion + this.personProportion + this.bidProportion;

    this.configuration = configuration;

    this.interEventDelayUs = new double[1];
    this.interEventDelayUs[0] = 1000000.0  / configuration.firstEventRate  * configuration.numEventGenerators;
    this.stepLengthSec = configuration.rateShape.stepLengthSec(configuration.ratePeriodSec);
    this.baseTime = baseTime;
    this.firstEventId = firstEventId;
    if (maxEventsOrZero == 0) {
      // Scale maximum down to avoid overflow in getEstimatedSizeBytes.
      this.maxEvents =
          Long.MAX_VALUE
              / (totalProportion
                  * Math.max(
                      Math.max(configuration.avgPersonByteSize, configuration.avgAuctionByteSize),
                      configuration.avgBidByteSize));
    } else {
      this.maxEvents = maxEventsOrZero;
    }
    this.firstEventNumber = firstEventNumber;

    long eventsPerEpoch = 0;
    long epochPeriodMs = 0;
    this.eventsPerEpoch = eventsPerEpoch;
    this.epochPeriodMs = epochPeriodMs;
  }

  /** Return a copy of this config. */
  public GeneratorConfig copy() {
    GeneratorConfig result;
    result =
        new GeneratorConfig(configuration, baseTime, firstEventId, maxEvents, firstEventNumber);
    return result;
  }

  /**
   * Split this config into {@code n} sub-configs with roughly equal number of possible events, but
   * distinct value spaces. The generators will run on parallel timelines. This config should no
   * longer be used.
   */
  public List<GeneratorConfig> split(int n) {
    List<GeneratorConfig> results = new ArrayList<>();
    if (n == 1) {
      // No split required.
      results.add(this);
    } else {
      long subMaxEvents = maxEvents / n;
      long subFirstEventId = firstEventId;
      for (int i = 0; i < n; i++) {
        if (i == n - 1) {
          // Don't loose any events to round-down.
          subMaxEvents = maxEvents - subMaxEvents * (n - 1);
        }
        results.add(copyWith(subFirstEventId, subMaxEvents, firstEventNumber));
        subFirstEventId += subMaxEvents;
      }
    }
    return results;
  }

  /** Return copy of this config except with given parameters. */
  public GeneratorConfig copyWith(long firstEventId, long maxEvents, long firstEventNumber) {
    return new GeneratorConfig(
            configuration, baseTime, firstEventId, maxEvents, firstEventNumber);
  }

  /** Return an estimate of the bytes needed by {@code numEvents}. */
  public long estimatedBytesForEvents(long numEvents) {
    long numPersons =
        (numEvents * personProportion) / totalProportion;
    long numAuctions = (numEvents * auctionProportion) / totalProportion;
    long numBids = (numEvents * bidProportion) / totalProportion;
    return numPersons * configuration.avgPersonByteSize
        + numAuctions * configuration.avgAuctionByteSize
        + numBids * configuration.avgBidByteSize;
  }

  public int getAvgPersonByteSize() {
    return configuration.avgPersonByteSize;
  }

  public int getNumActivePeople() {
    return configuration.numActivePeople;
  }

  public int getHotSellersRatio() {
    return configuration.hotSellersRatio;
  }

  public int getNumInFlightAuctions() {
    return configuration.numInFlightAuctions;
  }

  public int getHotAuctionRatio() {
    return configuration.hotAuctionRatio;
  }

  public int getHotBiddersRatio() {
    return configuration.hotBiddersRatio;
  }

  public int getAvgBidByteSize() {
    return configuration.avgBidByteSize;
  }

  public int getAvgAuctionByteSize() {
    return configuration.avgAuctionByteSize;
  }

  public double getProbDelayedEvent() {
    return configuration.probDelayedEvent;
  }

  public long getOccasionalDelaySec() {
    return configuration.occasionalDelaySec;
  }

  /** Return an estimate of the byte-size of all events a generator for this config would yield. */
  public long getEstimatedSizeBytes() {
    return estimatedBytesForEvents(maxEvents);
  }

  /**
   * Return the first 'event id' which could be generated from this config. Though events don't have
   * ids we can simulate them to help bookkeeping.
   */
  public long getStartEventId() {
    return firstEventId + firstEventNumber;
  }

  /** Return one past the last 'event id' which could be generated from this config. */
  public long getStopEventId() {
    return firstEventId + firstEventNumber + maxEvents;
  }

  /** Return the next event number for a generator which has so far emitted {@code numEvents}. */
  public long nextEventNumber(long numEvents) {
    return firstEventNumber + numEvents;
  }

  /**
   * Return the next event number for a generator which has so far emitted {@code numEvents}, but
   * adjusted to account for {@code outOfOrderGroupSize}.
   */
  public long nextAdjustedEventNumber(long numEvents) {
    long n = configuration.outOfOrderGroupSize;
    long eventNumber = nextEventNumber(numEvents);
    long base = (eventNumber / n) * n;
    long offset = (eventNumber * 953) % n;
    return base + offset;
  }

  /**
   * Return the event number who's event time will be a suitable watermark for a generator which has
   * so far emitted {@code numEvents}.
   */
  public long nextEventNumberForWatermark(long numEvents) {
    long n = configuration.outOfOrderGroupSize;
    long eventNumber = nextEventNumber(numEvents);
    return (eventNumber / n) * n;
  }

  /**
   * What timestamp should the event with {@code eventNumber} have for this generator?
   */
  public long timestampForEvent(long eventNumber) {
      return baseTime + (long)(eventNumber * interEventDelayUs[0]) / 1000L;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GeneratorConfig that = (GeneratorConfig) o;
    return personProportion == that.personProportion &&
        auctionProportion == that.auctionProportion &&
        bidProportion == that.bidProportion &&
        totalProportion == that.totalProportion &&
        stepLengthSec == that.stepLengthSec &&
        firstEventId == that.firstEventId &&
        maxEvents == that.maxEvents &&
        firstEventNumber == that.firstEventNumber &&
        epochPeriodMs == that.epochPeriodMs &&
        eventsPerEpoch == that.eventsPerEpoch &&
        Objects.equals(configuration, that.configuration) &&
        Arrays.equals(interEventDelayUs, that.interEventDelayUs);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(personProportion, auctionProportion, bidProportion, totalProportion, configuration, stepLengthSec, firstEventId, maxEvents, firstEventNumber, epochPeriodMs, eventsPerEpoch);
    result = 31 * result + Arrays.hashCode(interEventDelayUs);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("GeneratorConfig");
    sb.append("{configuration:");
    sb.append(configuration.toString());
    sb.append(";interEventDelayUs=[");
    for (int i = 0; i < interEventDelayUs.length; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(interEventDelayUs[i]);
    }
    sb.append("]");
    sb.append(";stepLengthSec:");
    sb.append(stepLengthSec);
    sb.append(";baseTime:");
    sb.append(baseTime);
    sb.append(";firstEventId:");
    sb.append(firstEventId);
    sb.append(";maxEvents:");
    sb.append(maxEvents);
    sb.append(";firstEventNumber:");
    sb.append(firstEventNumber);
    sb.append(";epochPeriodMs:");
    sb.append(epochPeriodMs);
    sb.append(";eventsPerEpoch:");
    sb.append(eventsPerEpoch);
    sb.append("}");
    return sb.toString();
  }
}
