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

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * An event in the auction system, either a (new) {@link Person}, a (new) {@link Auction}, or a
 * {@link Bid}.
 */
public class Event {

	public @Nullable Person newPerson;
	public @Nullable Auction newAuction;
	public @Nullable Bid bid;
	public Type type;

	/** The type of object stored in this event. * */
	public enum Type {
		PERSON(0),
		AUCTION(1),
		BID(2);

		public final int value;

		Type(int value) {
			this.value = value;
		}
	}

	public Event(Person newPerson) {
		this.newPerson = newPerson;
		newAuction = null;
		bid = null;
		type = Type.PERSON;
	}

	public Event(Auction newAuction) {
		newPerson = null;
		this.newAuction = newAuction;
		bid = null;
		type = Type.AUCTION;
	}

	public Event(Bid bid) {
		newPerson = null;
		newAuction = null;
		this.bid = bid;
		type = Type.BID;
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Event event = (Event) o;
		return Objects.equals(newPerson, event.newPerson)
			&& Objects.equals(newAuction, event.newAuction)
			&& Objects.equals(bid, event.bid);
	}

	@Override
	public int hashCode() {
		return Objects.hash(newPerson, newAuction, bid);
	}

	@Override
	public String toString() {
		if (newPerson != null) {
			return newPerson.toString();
		} else if (newAuction != null) {
			return newAuction.toString();
		} else if (bid != null) {
			return bid.toString();
		} else {
			throw new RuntimeException("invalid event");
		}
	}
}
