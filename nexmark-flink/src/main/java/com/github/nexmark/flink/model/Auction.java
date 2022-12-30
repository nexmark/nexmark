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

/** An auction submitted by a person. */
public class Auction implements Serializable {

	/** Id of auction. */
	public long id; // primary key

	/** Extra auction properties. */
	public String itemName;

	public String description;

	/** Initial bid price, in cents. */
	public long initialBid;

	/** Reserve price, in cents. */
	public long reserve;

	public Instant dateTime;

	/** When does auction expire? (ms since epoch). Bids at or after this time are ignored. */
	public Instant expires;

	/** Id of person who instigated auction. */
	public long seller; // foreign key: Person.id

	/** Id of category auction is listed under. */
	public long category; // foreign key: Category.id

	/** Additional arbitrary payload for performance testing. */
	public String extra;

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
		return "Auction{" +
				"id=" + id +
				", itemName='" + itemName + '\'' +
				", description='" + description + '\'' +
				", initialBid=" + initialBid +
				", reserve=" + reserve +
				", dateTime=" + dateTime +
				", expires=" + expires +
				", seller=" + seller +
				", category=" + category +
				", extra='" + extra + '\'' +
				'}';
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
			&& Objects.equals(dateTime, auction.dateTime)
			&& Objects.equals(expires, auction.expires)
			&& seller == auction.seller
			&& category == auction.category
			&& Objects.equals(itemName, auction.itemName)
			&& Objects.equals(description, auction.description)
			&& Objects.equals(extra, auction.extra);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra);
	}
}
