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

import com.github.nexmark.flink.generator.model.AuctionGenerator;
import com.github.nexmark.flink.generator.model.PersonGenerator;
import com.github.nexmark.flink.model.Bid;
import com.github.nexmark.flink.model.Event;
import com.github.nexmark.flink.generator.model.BidGenerator;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Objects;
import java.util.SplittableRandom;

import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * A generator for synthetic events. We try to make the data vaguely reasonable. We also ensure most
 * primary key/foreign key relations are correct. Eg: a {@link Bid} event will usually have valid
 * auction and bidder ids which can be joined to already-generated Auction and Person events.
 *
 * <p>To help with testing, we generate timestamps relative to a given {@code baseTime}. Each new
 * event is given a timestamp advanced from the previous timestamp by {@code interEventDelayUs} (in
 * microseconds). The event stream is thus fully deterministic and does not depend on wallclock
 * time.
 */
public class NexmarkGenerator implements Iterator<NexmarkGenerator.NextEvent>, Serializable {

  /**
   * The next event and its various timestamps. Ordered by increasing wallclock timestamp, then
   * (arbitrary but stable) event hash order.
   */
  public static class NextEvent implements Comparable<NextEvent> {
    /** When, in wallclock time, should this event be emitted? */
    public final long wallclockTimestamp;

    /** When, in event time, should this event be considered to have occured? */
    public final long eventTimestamp;

    /** The event itself. */
    public final Event event;

    /** The minimum of this and all future event timestamps. */
    public final long watermark;

    public NextEvent(long wallclockTimestamp, long eventTimestamp, Event event, long watermark) {
      this.wallclockTimestamp = wallclockTimestamp;
      this.eventTimestamp = eventTimestamp;
      this.event = event;
      this.watermark = watermark;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      NextEvent nextEvent = (NextEvent) o;

      return (wallclockTimestamp == nextEvent.wallclockTimestamp
          && eventTimestamp == nextEvent.eventTimestamp
          && watermark == nextEvent.watermark
          && event.equals(nextEvent.event));
    }

    @Override
    public int hashCode() {
      return Objects.hash(wallclockTimestamp, eventTimestamp, watermark, event);
    }

    @Override
    public int compareTo(NextEvent other) {
      int i = Long.compare(wallclockTimestamp, other.wallclockTimestamp);
      if (i != 0) {
        return i;
      }
      return Integer.compare(event.hashCode(), other.event.hashCode());
    }
  }

  private final SplittableRandom random = new SplittableRandom();

  /**
   * Configuration to generate events against. Note that it may be replaced by a call to {@link
   * #splitAtEventId}.
   */
  private GeneratorConfig config;

  /** Number of events generated by this generator. */
  private long eventsCountSoFar;

  /** Wallclock time at which we emitted the first event (ms since epoch). Initially -1. */
  private long wallclockBaseTime;

  public NexmarkGenerator(GeneratorConfig config, long eventsCountSoFar, long wallclockBaseTime) {
    checkNotNull(config);
    this.config = config;
    this.eventsCountSoFar = eventsCountSoFar;
    this.wallclockBaseTime = wallclockBaseTime;
  }

  /** Create a fresh generator according to {@code config}. */
  public NexmarkGenerator(GeneratorConfig config) {
    this(config, 0, -1);
  }

  /** Return a deep copy of this generator. */
  public NexmarkGenerator copy() {
    checkNotNull(config);
    return new NexmarkGenerator(config, eventsCountSoFar, wallclockBaseTime);
  }

  /**
   * Return the current config for this generator. Note that configs may be replaced by {@link
   * #splitAtEventId}.
   */
  public GeneratorConfig getCurrentConfig() {
    return config;
  }

  /**
   * Mutate this generator so that it will only generate events up to but not including {@code
   * eventId}. Return a config to represent the events this generator will no longer yield. The
   * generators will run in on a serial timeline.
   */
  public GeneratorConfig splitAtEventId(long eventId) {
    long newMaxEvents = eventId - (config.firstEventId + config.firstEventNumber);
    GeneratorConfig remainConfig =
        config.copyWith(
            config.firstEventId,
            config.maxEvents - newMaxEvents,
            config.firstEventNumber + newMaxEvents);
    config = config.copyWith(config.firstEventId, newMaxEvents, config.firstEventNumber);
    return remainConfig;
  }

  /**
   * Return the next 'event id'. Though events don't have ids we can simulate them to help with
   * bookkeeping.
   */
  public long getNextEventId() {
    return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
  }

  @Override
  public boolean hasNext() {
    return eventsCountSoFar < config.maxEvents;
  }

  /**
   * Return the next event. The outer timestamp is in wallclock time and corresponds to when the
   * event should fire. The inner timestamp is in event-time and represents the time the event is
   * purported to have taken place in the simulation.
   */
  public NextEvent nextEvent() {
    if (wallclockBaseTime < 0) {
      wallclockBaseTime = System.currentTimeMillis();
    }
    // When, in event time, we should generate the event. Monotonic.
    long eventTimestamp = config.timestampForEvent(config.nextEventNumber(eventsCountSoFar));
    // When, in event time, the event should say it was generated. Depending on outOfOrderGroupSize
    // may have local jitter.
    long adjustedEventTimestamp = config.timestampForEvent(config.nextAdjustedEventNumber(eventsCountSoFar));
    // The minimum of this and all future adjusted event timestamps. Accounts for jitter in
    // the event timestamp.
    long watermark = config.timestampForEvent(config.nextEventNumberForWatermark(eventsCountSoFar));
    // When, in wallclock time, we should emit the event.
    long wallclockTimestamp = wallclockBaseTime + (eventTimestamp - getCurrentConfig().baseTime);

    long newEventId = getNextEventId();
    long rem = newEventId % config.totalProportion;

    Event event;
    if (rem < config.personProportion) {
      event =
          new Event(PersonGenerator.nextPerson(newEventId, random, adjustedEventTimestamp, config));
    } else if (rem < config.personProportion + config.auctionProportion) {
      event =
          new Event(
              AuctionGenerator.nextAuction(eventsCountSoFar, newEventId, random, adjustedEventTimestamp, config));
    } else {
      event = new Event(BidGenerator.nextBid(newEventId, random, adjustedEventTimestamp, config));
    }

    eventsCountSoFar++;
    return new NextEvent(wallclockTimestamp, adjustedEventTimestamp, event, watermark);
  }

  @Override
  public NexmarkGenerator.NextEvent next() {
    return nextEvent();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /** Return an estimate of fraction of output consumed. */
  public double getFractionConsumed() {
    return (double) eventsCountSoFar / config.maxEvents;
  }

  /**
   * Gets Number of events generated by this generator.
   */
  public long getEventsCountSoFar() {
    return eventsCountSoFar;
  }

  @Override
  public String toString() {
    return String.format(
        "Generator{config:%s; eventsCountSoFar:%d; wallclockBaseTime:%d}",
        config, eventsCountSoFar, wallclockBaseTime);
  }
}
