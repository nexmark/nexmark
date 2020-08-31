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

import org.apache.flink.shaded.guava18.com.google.common.base.Objects;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import com.github.nexmark.flink.utils.NexmarkUtils;

import java.io.Serializable;
import java.time.Instant;

/** An auction submitted by a person. */
public class Auction implements Serializable {

	/** Id of auction. */
	@JsonProperty public long id; // primary key

	/** Extra auction properties. */
	@JsonProperty public String itemName;

	@JsonProperty public String description;

	/** Initial bid price, in cents. */
	@JsonProperty public long initialBid;

	/** Reserve price, in cents. */
	@JsonProperty public long reserve;

	@JsonProperty public Instant dateTime;

	/** When does auction expire? (ms since epoch). Bids at or after this time are ignored. */
	@JsonProperty public Instant expires;

	/** Id of person who instigated auction. */
	@JsonProperty public long seller; // foreign key: Person.id

	/** Id of category auction is listed under. */
	@JsonProperty public long category; // foreign key: Category.id

	/** Additional arbitrary payload for performance testing. */
	@JsonProperty public String extra;

	public Auction(
			long id,
			String itemName,
			String description,
			long initialBid,
			long reserve,
			Instant dateTime,
			Instant expires,
			long seller,
			long category,
			String extra) {
		this.id = id;
		this.itemName = itemName;
		this.description = description;
		this.initialBid = initialBid;
		this.reserve = reserve;
		this.dateTime = dateTime;
		this.expires = expires;
		this.seller = seller;
		this.category = category;
		this.extra = extra;
	}

	@Override
	public String toString() {
		try {
			return NexmarkUtils.MAPPER.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Auction auction = (Auction) o;
		return id == auction.id
			&& initialBid == auction.initialBid
			&& reserve == auction.reserve
			&& Objects.equal(dateTime, auction.dateTime)
			&& Objects.equal(expires, auction.expires)
			&& seller == auction.seller
			&& category == auction.category
			&& Objects.equal(itemName, auction.itemName)
			&& Objects.equal(description, auction.description)
			&& Objects.equal(extra, auction.extra);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(
			id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra);
	}
}
