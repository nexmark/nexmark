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

import java.util.SplittableRandom;

/**
 * Samples ids from a Zipf distribution over {@code [0, n)} in O(1) time and O(1) memory, so it
 * works for millions of keys without a lookup table.
 *
 * <p>P(rank k) is proportional to {@code 1 / k^exponent} for k = 1..n. Rank 1 is the hottest key.
 * Sampling uses rejection-inversion (Hormann and Derflinger, 1996), the same algorithm as Apache
 * Commons Math's {@code ZipfDistribution}.
 *
 * <p>Ranks are mapped to ids through a bijective affine scramble ({@code id = (rank-1) * m mod n}
 * with {@code gcd(m, n) = 1}). Without it the hot keys would be the numerically smallest ids,
 * which sit next to each other in the key-sorted SST files and would make block-cache locality
 * look better than for a real skewed workload. Rank 1 always maps to id 0.
 */
public final class ZipfSampler {

  private final long n;
  private final double exponent;
  private final double hIntegralX1;
  private final double hIntegralN;
  private final double s;
  private final long multiplier;

  public ZipfSampler(long n, double exponent) {
    if (n < 1) {
      throw new IllegalArgumentException("n must be >= 1, got " + n);
    }
    if (!(exponent > 0)) {
      throw new IllegalArgumentException("zipf exponent must be > 0, got " + exponent);
    }
    this.n = n;
    this.exponent = exponent;
    this.hIntegralX1 = hIntegral(1.5) - 1.0;
    this.hIntegralN = hIntegral(n + 0.5);
    this.s = 2.0 - hIntegralInverse(hIntegral(2.5) - h(2.0));
    this.multiplier = coprimeMultiplier(n);
  }

  /** Returns a rank in {@code [1, n]}; 1 is the hottest. */
  public long sampleRank(SplittableRandom random) {
    while (true) {
      double u = hIntegralN + random.nextDouble() * (hIntegralX1 - hIntegralN);
      double x = hIntegralInverse(u);
      long k = (long) (x + 0.5);
      if (k < 1) {
        k = 1;
      } else if (k > n) {
        k = n;
      }
      if (k - x <= s || u >= hIntegral(k + 0.5) - h(k)) {
        return k;
      }
    }
  }

  /** Returns an id in {@code [0, n)}, with hot keys scattered across the id space. */
  public long sampleId(SplittableRandom random) {
    return rankToId(sampleRank(random));
  }

  /** The bijection from rank (1..n) to id (0..n-1). */
  public long rankToId(long rank) {
    return ((rank - 1) * multiplier) % n;
  }

  public long size() {
    return n;
  }

  /**
   * For each fraction f, the smallest number of top-ranked keys that together receive at least a
   * fraction f of all lookups. Costs O(n); intended for a one-off startup report.
   */
  public static long[] coverage(long n, double exponent, double... fractions) {
    double total = 0;
    for (long k = 1; k <= n; k++) {
      total += Math.pow(k, -exponent);
    }
    long[] result = new long[fractions.length];
    int next = 0;
    double cumulative = 0;
    for (long k = 1; k <= n && next < fractions.length; k++) {
      cumulative += Math.pow(k, -exponent);
      while (next < fractions.length && cumulative >= fractions[next] * total) {
        result[next++] = k;
      }
    }
    while (next < fractions.length) {
      result[next++] = n;
    }
    return result;
  }

  // ---- rejection-inversion helpers --------------------------------------------------------

  private double hIntegral(double x) {
    double logX = Math.log(x);
    return helper2((1.0 - exponent) * logX) * logX;
  }

  private double h(double x) {
    return Math.exp(-exponent * Math.log(x));
  }

  private double hIntegralInverse(double x) {
    double t = x * (1.0 - exponent);
    if (t < -1.0) {
      t = -1.0;
    }
    return Math.exp(helper1(t) * x);
  }

  /** log(1 + x) / x, stable near 0. */
  private static double helper1(double x) {
    if (Math.abs(x) > 1e-8) {
      return Math.log1p(x) / x;
    }
    return 1.0 - x * (0.5 - x * (1.0 / 3.0 - 0.25 * x));
  }

  /** (exp(x) - 1) / x, stable near 0. */
  private static double helper2(double x) {
    if (Math.abs(x) > 1e-8) {
      return Math.expm1(x) / x;
    }
    return 1.0 + x * 0.5 * (1.0 + x / 3.0 * (1.0 + 0.25 * x));
  }

  private static long coprimeMultiplier(long n) {
    if (n <= 2) {
      return 1;
    }
    long m = ((long) (n * 0.6180339887498949)) | 1L;
    while (gcd(m, n) != 1) {
      m += 2;
    }
    return m;
  }

  private static long gcd(long a, long b) {
    while (b != 0) {
      long t = a % b;
      a = b;
      b = t;
    }
    return a;
  }
}
