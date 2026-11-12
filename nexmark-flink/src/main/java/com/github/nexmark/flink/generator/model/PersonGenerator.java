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

import com.github.nexmark.flink.model.Person;
import com.github.nexmark.flink.generator.GeneratorConfig;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;

import static com.github.nexmark.flink.generator.model.StringsGenerator.nextExtra;
import static com.github.nexmark.flink.generator.model.StringsGenerator.nextString;

/** Generates people. */
public class PersonGenerator {
  /**
   * Keep the number of states small so that the example queries will find results even with a small
   * batch of events.
   */
  private static final List<String> US_STATES = Arrays.asList("AZ,CA,ID,OR,WA,WY".split(","));

  private static final List<String> US_CITIES =
      Arrays.asList(
          "Phoenix,Los Angeles,San Francisco,Boise,Portland,Bend,Redmond,Seattle,Kent,Cheyenne"
              .split(","));

  private static final List<String> FIRST_NAMES =
      Arrays.asList("Peter,Paul,Luke,John,Saul,Vicky,Kate,Julie,Sarah,Deiter,Walter".split(","));

  private static final List<String> LAST_NAMES =
      Arrays.asList("Shultz,Abrams,Spencer,White,Bartels,Walton,Smith,Jones,Noris".split(","));

  private static final String[] CREDIT_CARD_STRINGS = createCreditCardStrings();

  /** Generate and return a random person with next available id. */
  public static Person nextPerson(
          long nextEventId, SplittableRandom random, long timestamp, GeneratorConfig config) {

    long id = lastBase0PersonId(config, nextEventId) + GeneratorConfig.FIRST_PERSON_ID;
    String name = nextPersonName(random);
    String email = nextEmail(random);
    String creditCard = nextCreditCard(random);
    String city = nextUSCity(random);
    String state = nextUSState(random);
    int currentSize =
        8 + name.length() + email.length() + creditCard.length() + city.length() + state.length();
    String extra = nextExtra(
        random, currentSize, config.getAvgPersonByteSize(), config.getCompressibleExtra());
    return new Person(id, name, email, creditCard, city, state, Instant.ofEpochMilli(timestamp), extra);
  }

  /** Return a random person id (base 0). */
  public static long nextBase0PersonId(long eventId, SplittableRandom random, GeneratorConfig config) {
    // EXPERIMENT: fixed active window.
    // Draw the seller/bidder uniformly from the fixed prefix of numActivePeople Persons, independent of
    // how many persons have been generated so far. This pins the set of join keys to a stable
    // prefix for the whole run and across savepoint restores, and it still works when person
    // generation is disabled (personProportion = 0) in the measurement phase.
    //
    // Original upstream (sliding window) behaviour, kept for reference:
    //   long numPeople = lastBase0PersonId(config, eventId) + 1;
    //   long activePeople = Math.min(numPeople, config.getNumActivePeople());
    //   long n = LongGenerator.nextLong(random, activePeople + PERSON_ID_LEAD);  // PERSON_ID_LEAD = 10
    //   return numPeople - activePeople + n;
    //
    // Upstream's PERSON_ID_LEAD of 10 is deliberately NOT used: it let auctions name sellers that
    // did not exist yet, to model out-of-order arrival. Here every key already exists in the
    // savepoint, so the lead would only widen the key range by 10 and make numActivePeople
    // inexact. Sellers are drawn from exactly numActivePeople distinct keys (see below for where they start).
    //
    // The first Person that exists does NOT have base id 0. NexmarkQ3Job builds its generator with
    // firstEventId = 1 and firstEventNumber = 1, and an event's id is firstEventId + firstEventNumber
    // + its index, so the Persons in state have base ids 2, 3, 4, ... (ids 1002, 1003, ...). Drawing
    // from [0, n) therefore named two sellers that never exist, so about 2/n of all lookups found
    // nothing (measured: 0.0202% against 0.0200% predicted for n = 10 000). Draw from the range
    // where Persons actually exist instead.
    long domain = config.getNumActivePeople();
    long draw = LongGenerator.nextLong(random, domain);
    return FIRST_PERSON_BASE0 + draw;
  }

  /**
   * Base id of the first Person in state: the Persons made by the Q3 job's build have base ids
   * {@code FIRST_PERSON_BASE0 ... FIRST_PERSON_BASE0 + persons - 1}. See {@link #nextBase0PersonId}.
   */
  public static final long FIRST_PERSON_BASE0 = 2L;

  /**
   * Return the last valid person id (ignoring FIRST_PERSON_ID). Will be the current person id if
   * due to generate a person.
   */
  public static long lastBase0PersonId(GeneratorConfig config, long eventId) {
    long epoch = eventId / config.totalProportion;
    long offset = eventId % config.totalProportion;
    if (offset >= config.personProportion) {
      // About to generate an auction or bid.
      // Go back to the last person generated in this epoch.
      offset = config.personProportion - 1;
    }
    // About to generate a person.
    return epoch * config.personProportion + offset;
  }

  /** return a random US state. */
  private static String nextUSState(SplittableRandom random) {
    return US_STATES.get(random.nextInt(US_STATES.size()));
  }

  /** Return a random US city. */
  private static String nextUSCity(SplittableRandom random) {
    return US_CITIES.get(random.nextInt(US_CITIES.size()));
  }

  /** Return a random person name. */
  private static String nextPersonName(SplittableRandom random) {
    return FIRST_NAMES.get(random.nextInt(FIRST_NAMES.size()))
        + " "
        + LAST_NAMES.get(random.nextInt(LAST_NAMES.size()));
  }

  /** Return a random email address. */
  private static String nextEmail(SplittableRandom random) {
    return nextString(random, 7) + "@" + nextString(random, 5) + ".com";
  }

  private static String[] createCreditCardStrings() {
    String[] creditCardStrings = new String[10000];
    for (int i = 0; i < creditCardStrings.length; ++i) {
      creditCardStrings[i] = String.format("%04d", i);
    }
    return creditCardStrings;
  }

  /** Return a random credit card number. */
  private static String nextCreditCard(SplittableRandom random) {
    StringBuilder sb = new StringBuilder(20);
    for (int i = 0; i < 4; i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append(CREDIT_CARD_STRINGS[random.nextInt(CREDIT_CARD_STRINGS.length)]);
    }
    return sb.toString();
  }
}
