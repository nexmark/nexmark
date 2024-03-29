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

import java.util.SplittableRandom;

/** Generates strings which are used for different field in other model objects. */
public class StringsGenerator {

  /** Smallest random string size. */
  private static final int MIN_STRING_LENGTH = 3;

  private static final String REUSABLE_EXTRA_STRING = nextExactString(new SplittableRandom(), 1024 * 1024);

  /** Return a random string of up to {@code maxLength}. */
  public static String nextString(SplittableRandom random, int maxLength) {
    return nextString(random, maxLength, ' ');
  }

  public static String nextString(SplittableRandom random, int maxLength, char special) {
    int len = MIN_STRING_LENGTH + random.nextInt(maxLength - MIN_STRING_LENGTH);
    StringBuilder sb = new StringBuilder(len);
    while (len-- > 0) {
      if (random.nextInt(13) == 0) {
        sb.append(special);
      } else {
        sb.append((char) ('a' + random.nextInt(26)));
      }
    }
    return sb.toString().trim();
  }

  /** Return a random string of exactly {@code length}. */
  public static String nextExactString(SplittableRandom random, int length) {
    if (REUSABLE_EXTRA_STRING != null && length < REUSABLE_EXTRA_STRING.length() / 2) {
      int offset = random.nextInt(REUSABLE_EXTRA_STRING.length() - length);
      return REUSABLE_EXTRA_STRING.substring(offset, offset + length);
    }

    StringBuilder sb = new StringBuilder(length);
    int rnd = 0;
    int n = 0; // number of random characters left in rnd
    while (length-- > 0) {
      if (n == 0) {
        rnd = random.nextInt();
        n = 6; // log_26(2^31)
      }
      sb.append((char) ('a' + rnd % 26));
      rnd /= 26;
      n--;
    }
    return sb.toString();
  }

  /**
   * Return a random {@code string} such that {@code currentSize + string.length()} is on average
   * {@code averageSize}.
   */
  public static String nextExtra(SplittableRandom random, int currentSize, int desiredAverageSize) {
    if (currentSize > desiredAverageSize) {
      return "";
    }
    desiredAverageSize -= currentSize;
    int delta = (int) Math.round(desiredAverageSize * 0.2);
    int minSize = desiredAverageSize - delta;
    int desiredSize = minSize + (delta == 0 ? 0 : random.nextInt(2 * delta));
    return nextExactString(random, desiredSize);
  }
}
