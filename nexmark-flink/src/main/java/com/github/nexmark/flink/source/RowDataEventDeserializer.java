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

package com.github.nexmark.flink.source;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import com.github.nexmark.flink.model.Auction;
import com.github.nexmark.flink.model.Bid;
import com.github.nexmark.flink.model.Event;
import com.github.nexmark.flink.model.Person;

public class RowDataEventDeserializer implements EventDeserializer<RowData> {

	@Override
	public RowData deserialize(Event event) {
		return convertEvent(event);
	}

	private RowData convertEvent(Event event) {
		GenericRowData rowData = new GenericRowData(4);
		rowData.setField(0, event.type.value);
		if (event.type == Event.Type.PERSON) {
			assert event.newPerson != null;
			rowData.setField(1, convertPerson(event.newPerson));
		} else if (event.type == Event.Type.AUCTION) {
			assert event.newAuction != null;
			rowData.setField(2, convertAuction(event.newAuction));
		} else if (event.type == Event.Type.BID) {
			assert event.bid != null;
			rowData.setField(3, convertBid(event.bid));
		} else {
			throw new UnsupportedOperationException("Unsupported event type: " + event.type.name());
		}
		return rowData;
	}

	private RowData convertPerson(Person person) {
		GenericRowData rowData = new GenericRowData(8);
		rowData.setField(0, person.id);
		rowData.setField(1, StringData.fromString(person.name));
		rowData.setField(2, StringData.fromString(person.emailAddress));
		rowData.setField(3, StringData.fromString(person.creditCard));
		rowData.setField(4, StringData.fromString(person.city));
		rowData.setField(5, StringData.fromString(person.state));
		rowData.setField(6, TimestampData.fromInstant(person.dateTime));
		rowData.setField(7, StringData.fromString(person.extra));
		return rowData;
	}

	private RowData convertAuction(Auction auction) {
		GenericRowData rowData = new GenericRowData(10);
		rowData.setField(0, auction.id);
		rowData.setField(1, StringData.fromString(auction.itemName));
		rowData.setField(2, StringData.fromString(auction.description));
		rowData.setField(3, auction.initialBid);
		rowData.setField(4, auction.reserve);
		rowData.setField(5, TimestampData.fromInstant(auction.dateTime));
		rowData.setField(6, TimestampData.fromInstant(auction.expires));
		rowData.setField(7, auction.seller);
		rowData.setField(8, auction.category);
		rowData.setField(9, StringData.fromString(auction.extra));
		return rowData;
	}

	private RowData convertBid(Bid bid) {
		GenericRowData rowData = new GenericRowData(7);
		rowData.setField(0, bid.auction);
		rowData.setField(1, bid.bidder);
		rowData.setField(2, bid.price);
		rowData.setField(3, StringData.fromString(bid.channel));
		rowData.setField(4, StringData.fromString(bid.url));
		rowData.setField(5, TimestampData.fromInstant(bid.dateTime));
		rowData.setField(6, StringData.fromString(bid.extra));
		return rowData;
	}

}
