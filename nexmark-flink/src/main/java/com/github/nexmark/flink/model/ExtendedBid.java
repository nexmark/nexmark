package com.github.nexmark.flink.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import com.github.nexmark.flink.utils.NexmarkUtils;

import java.time.Instant;
import java.util.Objects;

/** A extended bid for an item on auction. It also contains the seller and category information. */
public class ExtendedBid extends Bid {

	/** The seller id that binds with auction. */
	@JsonProperty public long seller;

	/** The category of the auction. */
	@JsonProperty public long category;

	public ExtendedBid(long auction, long seller, long bidder, long category, long price, Instant dateTime, String extra) {
		super(auction, bidder, price, dateTime, extra);
		this.seller = seller;
		this.category = category;
	}

	@Override
	public boolean equals(Object otherObject) {
		if (this == otherObject) {
			return true;
		}
		if (otherObject == null || getClass() != otherObject.getClass()) {
			return false;
		}

		ExtendedBid other = (ExtendedBid) otherObject;
		return Objects.equals(auction, other.auction)
			&& Objects.equals(bidder, other.bidder)
			&& Objects.equals(price, other.price)
			&& Objects.equals(dateTime, other.dateTime)
			&& Objects.equals(extra, other.extra)
			&& Objects.equals(seller, other.seller)
			&& Objects.equals(category, other.category);
	}

	@Override
	public int hashCode() {
		return Objects.hash(auction, seller, bidder, category, price, dateTime, extra);
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
