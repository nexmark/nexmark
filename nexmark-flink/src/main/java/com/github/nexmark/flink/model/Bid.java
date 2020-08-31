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

package com.github.nexmark.flink.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import com.github.nexmark.flink.utils.NexmarkUtils;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/** A bid for an item on auction. */
public class Bid implements Serializable {

	/** Id of auction this bid is for. */
	@JsonProperty public long auction; // foreign key: Auction.id

	/** Id of person bidding in auction. */
	@JsonProperty public long bidder; // foreign key: Person.id

	/** Price of bid, in cents. */
	@JsonProperty public long price;

	/**
	 * Instant at which bid was made (ms since epoch). NOTE: This may be earlier than the system's
	 * event time.
	 */
	@JsonProperty public Instant dateTime;

	/** Additional arbitrary payload for performance testing. */
	@JsonProperty public String extra;

	public Bid(long auction, long bidder, long price, Instant dateTime, String extra) {
		this.auction = auction;
		this.bidder = bidder;
		this.price = price;
		this.dateTime = dateTime;
		this.extra = extra;
	}

	@Override
	public boolean equals(Object otherObject) {
		if (this == otherObject) {
			return true;
		}
		if (otherObject == null || getClass() != otherObject.getClass()) {
			return false;
		}

		Bid other = (Bid) otherObject;
		return Objects.equals(auction, other.auction)
			&& Objects.equals(bidder, other.bidder)
			&& Objects.equals(price, other.price)
			&& Objects.equals(dateTime, other.dateTime)
			&& Objects.equals(extra, other.extra);
	}

	@Override
	public int hashCode() {
		return Objects.hash(auction, bidder, price, dateTime, extra);
	}

	@Override
	public String toString() {
		try {
			return NexmarkUtils.MAPPER.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
}
