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

package com.github.nexmark.flink.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Odd's 'n Ends used throughout queries and driver. */
public class NexmarkUtils {

	private static final Logger LOG = LoggerFactory.getLogger(NexmarkUtils.class);

	/** Mapper for (de)serializing JSON. */
	public static final ObjectMapper MAPPER = new ObjectMapper();

	/** Units for rates. */
	public enum RateUnit {
		PER_SECOND(1_000_000L),
		PER_MINUTE(60_000_000L);

		RateUnit(long usPerUnit) {
			this.usPerUnit = usPerUnit;
		}

		/** Number of microseconds per unit. */
		private final long usPerUnit;

		/** Number of microseconds between events at given rate. */
		public long rateToPeriodUs(long rate) {
			return (usPerUnit + rate / 2) / rate;
		}
	}

	/** Shape of event rate. */
	public enum RateShape {
		SQUARE,
		SINE;

		/** Number of steps used to approximate sine wave. */
		private static final int N = 10;

		/**
		 * Return inter-event delay, in microseconds, for each generator to follow in order to achieve
		 * {@code rate} at {@code unit} using {@code numGenerators}.
		 */
		public long interEventDelayUs(int rate, RateUnit unit, int numGenerators) {
			return unit.rateToPeriodUs(rate) * numGenerators;
		}

		/**
		 * Return array of successive inter-event delays, in microseconds, for each generator to follow
		 * in order to achieve this shape with {@code firstRate/nextRate} at {@code unit} using {@code
		 * numGenerators}.
		 */
		public long[] interEventDelayUs(int firstRate, int nextRate, RateUnit unit, int numGenerators) {
			if (firstRate == nextRate) {
				long[] interEventDelayUs = new long[1];
				interEventDelayUs[0] = unit.rateToPeriodUs(firstRate) * numGenerators;
				return interEventDelayUs;
			}

			switch (this) {
				case SQUARE:
				{
					long[] interEventDelayUs = new long[2];
					interEventDelayUs[0] = unit.rateToPeriodUs(firstRate) * numGenerators;
					interEventDelayUs[1] = unit.rateToPeriodUs(nextRate) * numGenerators;
					return interEventDelayUs;
				}
				case SINE:
				{
					double mid = (firstRate + nextRate) / 2.0;
					double amp = (firstRate - nextRate) / 2.0; // may be -ve
					long[] interEventDelayUs = new long[N];
					for (int i = 0; i < N; i++) {
						double r = (2.0 * Math.PI * i) / N;
						double rate = mid + amp * Math.cos(r);
						interEventDelayUs[i] = unit.rateToPeriodUs(Math.round(rate)) * numGenerators;
					}
					return interEventDelayUs;
				}
			}
			throw new RuntimeException(); // switch should be exhaustive
		}

		/**
		 * Return delay between steps, in seconds, for result of {@link #interEventDelayUs}, so as to
		 * cycle through the entire sequence every {@code ratePeriodSec}.
		 */
		public int stepLengthSec(int ratePeriodSec) {
			int n = 0;
			switch (this) {
				case SQUARE:
					n = 2;
					break;
				case SINE:
					n = N;
					break;
			}
			return (ratePeriodSec + n - 1) / n;
		}
	}
}
