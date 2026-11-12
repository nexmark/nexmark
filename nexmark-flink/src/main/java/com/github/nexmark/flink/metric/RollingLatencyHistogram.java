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

package com.github.nexmark.flink.metric;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

import java.util.function.LongSupplier;

/**
 * A latency histogram whose statistics cover the last {@value #WINDOW_SECONDS} seconds.
 *
 * <p>Flink's built-in histograms keep only a fixed number of the most recent samples (1,000 by
 * default). At tens of thousands of lookups per second that is a window of a few milliseconds, so
 * a metrics scrape every 15 s would see only a tiny, noisy slice. This implementation counts
 * values into log-linear buckets (about 3% relative precision, values 0 .. 2^41) kept per second,
 * so quantiles reflect the whole recent window at a fixed, small memory cost.
 *
 * <p>Values are unitless longs; callers choose the unit (the Q3 job records microseconds).
 * Updates come from the task thread while the metric reporter reads from another thread, so both
 * paths are synchronized; the critical section is a few array operations.
 */
public class RollingLatencyHistogram implements Histogram {

    public static final int WINDOW_SECONDS = 30;

    private static final int SUB_BITS = 5;
    private static final int SUB_BUCKETS = 1 << SUB_BITS;     // 32 sub-buckets per power of two
    private static final int LINEAR_LIMIT = 2 * SUB_BUCKETS;  // values below 64 get exact buckets
    private static final int MAX_EXP = 40;
    private static final int BUCKETS = LINEAR_LIMIT + (MAX_EXP - SUB_BITS) * SUB_BUCKETS;

    private final LongSupplier clockSeconds;
    private final long[][] counts = new long[WINDOW_SECONDS][BUCKETS];
    private final long[] slotSecond = new long[WINDOW_SECONDS];
    private long totalCount;

    public RollingLatencyHistogram() {
        this(() -> System.nanoTime() / 1_000_000_000L);
    }

    /** Visible for testing: inject the clock (in seconds). */
    RollingLatencyHistogram(LongSupplier clockSeconds) {
        this.clockSeconds = clockSeconds;
        java.util.Arrays.fill(slotSecond, Long.MIN_VALUE);
    }

    @Override
    public synchronized void update(long value) {
        long now = clockSeconds.getAsLong();
        int slot = (int) Math.floorMod(now, (long) WINDOW_SECONDS);
        if (slotSecond[slot] != now) {
            java.util.Arrays.fill(counts[slot], 0L);
            slotSecond[slot] = now;
        }
        counts[slot][bucketIndex(value < 0 ? 0 : value)]++;
        totalCount++;
    }

    /** Total number of values ever recorded (not just the window). */
    @Override
    public synchronized long getCount() {
        return totalCount;
    }

    @Override
    public synchronized HistogramStatistics getStatistics() {
        long now = clockSeconds.getAsLong();
        long[] merged = new long[BUCKETS];
        for (int slot = 0; slot < WINDOW_SECONDS; slot++) {
            long sec = slotSecond[slot];
            if (sec != Long.MIN_VALUE && sec > now - WINDOW_SECONDS && sec <= now) {
                long[] c = counts[slot];
                for (int i = 0; i < BUCKETS; i++) {
                    merged[i] += c[i];
                }
            }
        }
        return new Stats(merged);
    }

    // ---- bucket mapping ------------------------------------------------------------------

    static int bucketIndex(long v) {
        if (v < LINEAR_LIMIT) {
            return (int) v;
        }
        int e = 63 - Long.numberOfLeadingZeros(v);            // floor(log2 v), >= 6
        if (e > MAX_EXP) {
            return BUCKETS - 1;
        }
        int shift = e - SUB_BITS;
        int sub = (int) ((v >>> shift) - SUB_BUCKETS);         // 0 .. 31
        return LINEAR_LIMIT + (e - (SUB_BITS + 1)) * SUB_BUCKETS + sub;
    }

    /** Representative value (midpoint) of a bucket. */
    static double bucketValue(int index) {
        if (index < LINEAR_LIMIT) {
            return index;
        }
        int k = index - LINEAR_LIMIT;
        int e = k / SUB_BUCKETS + (SUB_BITS + 1);
        int sub = k % SUB_BUCKETS;
        int shift = e - SUB_BITS;
        long lower = ((long) (SUB_BUCKETS + sub)) << shift;
        return lower + (1L << shift) / 2.0;
    }

    // ---- immutable snapshot --------------------------------------------------------------

    private static final class Stats extends HistogramStatistics {
        private final long[] counts;
        private final long total;

        Stats(long[] counts) {
            this.counts = counts;
            long t = 0;
            for (long c : counts) {
                t += c;
            }
            this.total = t;
        }

        @Override
        public double getQuantile(double quantile) {
            if (total == 0) {
                return 0.0;
            }
            long target = Math.max(1L, (long) Math.ceil(Math.min(1.0, Math.max(0.0, quantile)) * total));
            long cumulative = 0;
            for (int i = 0; i < counts.length; i++) {
                cumulative += counts[i];
                if (cumulative >= target) {
                    return bucketValue(i);
                }
            }
            return bucketValue(counts.length - 1);
        }

        @Override
        public long[] getValues() {
            int n = (int) Math.min(total, 10_000L);
            long[] values = new long[n];
            for (int i = 0; i < n; i++) {
                values[i] = Math.round(getQuantile((i + 0.5) / n));
            }
            return values;
        }

        @Override
        public int size() {
            return (int) Math.min(total, Integer.MAX_VALUE);
        }

        @Override
        public double getMean() {
            if (total == 0) {
                return 0.0;
            }
            double sum = 0;
            for (int i = 0; i < counts.length; i++) {
                sum += counts[i] * bucketValue(i);
            }
            return sum / total;
        }

        @Override
        public double getStdDev() {
            if (total == 0) {
                return 0.0;
            }
            double mean = getMean();
            double acc = 0;
            for (int i = 0; i < counts.length; i++) {
                double d = bucketValue(i) - mean;
                acc += counts[i] * d * d;
            }
            return Math.sqrt(acc / total);
        }

        @Override
        public long getMax() {
            for (int i = counts.length - 1; i >= 0; i--) {
                if (counts[i] > 0) {
                    return Math.round(bucketValue(i));
                }
            }
            return 0L;
        }

        @Override
        public long getMin() {
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] > 0) {
                    return Math.round(bucketValue(i));
                }
            }
            return 0L;
        }
    }
}
