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

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/** A bid for an item on auction. */
public class Bid implements Serializable {

	/** Id of auction this bid is for. */
	public long auction; // foreign key: Auction.id

	/** Id of person bidding in auction. */
	public long bidder; // foreign key: Person.id

	/** Price of bid, in cents. */
	public long price;

	/** The channel introduced this bidding. */
	public String channel;

	/** The url of this bid. */
	public String url;

	/**
	 * Instant at which bid was made (ms since epoch). NOTE: This may be earlier than the system's
	 * event time.
	 */
	public Instant dateTime;

	/** Additional arbitrary payload for performance testing. */
	public String extra;

	public Bid(long auction, long bidder, long price, String channel, String url, Instant dateTime, String extra) {
		this.auction = auction;
		this.bidder = bidder;
		this.price = price;
		this.channel = channel;
		this.url = url;
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
			&& Objects.equals(channel, other.channel)
			&& Objects.equals(url, other.url)
			&& Objects.equals(dateTime, other.dateTime)
			&& Objects.equals(extra, other.extra);
	}

	@Override
	public int hashCode() {
		return Objects.hash(auction, bidder, price, channel, url, dateTime, extra);
	}

	@Override
	public String toString() {
		return "Bid{" +
				"auction=" + auction +
				", bidder=" + bidder +
				", price=" + price +
				", channel=" + channel +
				", url=" + url +
				", dateTime=" + dateTime +
				", extra='" + extra + '\'' +
				'}';
	}
}
