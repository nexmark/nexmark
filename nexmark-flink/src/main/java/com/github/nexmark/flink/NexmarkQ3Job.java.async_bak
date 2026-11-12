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

package com.github.nexmark.flink;

import com.github.nexmark.flink.generator.GeneratorConfig;
import com.github.nexmark.flink.model.Auction;
import com.github.nexmark.flink.model.Event;
import com.github.nexmark.flink.model.Person;
import com.github.nexmark.flink.source.NexmarkSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.util.function.ThrowingConsumer;

import java.time.Instant;

/**
 * Nexmark Query 3 - Local Item Suggestion (Java DataStream) - working-set / cache experiment build.
 *
 * <p>Base query: {@code auction A INNER JOIN person P ON A.seller = P.id}. The Q3 WHERE filters
 * ({@code A.category = 10}, {@code P.state IN ('OR','ID','CA')}) are intentionally REMOVED so that
 * every auction probes {@code personState} and state accumulates as fast as possible - this job is
 * an instrument, not a faithful Q3.
 *
 * <p>Two phases:
 * <ul>
 *   <li>{@code --phase build}   - generate persons, accumulate {@code personState} (the "address
 *       space"), then take a savepoint.
 *   <li>{@code --phase measure} - restore that savepoint, DROP all incoming Person events so
 *       {@code personState} is frozen, stream only auctions; each auction does one async
 *       {@code personState} point-read. Sweep {@code --working-set} to move the working set
 *       across the ForSt local cache.
 * </ul>
 *
 * <p>Async state access ({@code.enableAsyncState()} + v2 state API) matches the paper's
 * "Flink 2.0-HDFS-async" execution model.
 *
 * <p>CLI flags:
 * <pre>
 *   --events 500000        Total events to generate (ceiling; snapshot earlier in build phase)
 *   --working-set 1000     Fixed number of active seller keys  = the working set
 *   --parallelism 1        Job parallelism (keep low so per-subtask cache/state ratio is meaningful)
 *   --phase build|measure  build = accumulate state; measure = frozen state, auctions only
 *   --hot-sellers-ratio 1  MUST be 1 so every seller goes through the fixed active window
 *   --person-proportion / --auction-proportion / --bid-proportion
 *   --person-avg-size 200  Bytes of padding per Person - inflate to grow per-key state
 * </pre>
 */
public class NexmarkQ3Job {

    public static void main(String[] args) throws Exception {
        ParameterTool params  = ParameterTool.fromArgs(args);
        long    numEvents     = params.getLong("events", 500_000L);
        int     workingSet    = params.getInt("working-set", 1000);
        int     parallelism   = params.getInt("parallelism", 1);
        int     personProp    = params.getInt("person-proportion", 10);
        int     auctionProp   = params.getInt("auction-proportion", 4);
        int     bidProp       = params.getInt("bid-proportion", 36);
        int     hotSellers    = params.getInt("hot-sellers-ratio", 1);
        int     personAvgSize = params.getInt("person-avg-size", 200);
        boolean compressible  = params.getBoolean("compressible-extra", true);
        boolean measure       = params.get("phase", "build").equalsIgnoreCase("measure");

        System.out.println("=======================================================");
        System.out.println("  Nexmark Q3 - Java DataStream (working-set experiment)");
        System.out.println("=======================================================");
        System.out.printf("  phase          : %s%n", measure ? "measure (persons dropped)" : "build");
        System.out.printf("  events         : %,d%n", numEvents);
        System.out.printf("  working-set     : %d active seller keys%n", workingSet);
        System.out.printf("  parallelism     : %d%n", parallelism);
        System.out.printf("  proportions     : person=%d auction=%d bid=%d%n", personProp, auctionProp, bidProp);
        System.out.printf("  hot-sellers-ratio: %d%n", hotSellers);
        System.out.printf("  person-avg-size : %d bytes%n", personAvgSize);
        System.out.printf("  compressible-extra: %s%n", compressible);
        System.out.println("=======================================================");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        // -- Nexmark generator config -----------------------------------------
        NexmarkConfiguration nexmarkConf = new NexmarkConfiguration();
        nexmarkConf.numEvents          = numEvents;
        nexmarkConf.stopAtEvent        = -1L;
        nexmarkConf.isSourceKeepAlive  = false;
        int tps = params.getInt("tps", 0);
        if (tps > 0) {
            nexmarkConf.maxEmitSpeed = false;
            nexmarkConf.firstEventRate = tps;
            nexmarkConf.nextEventRate = tps;
        } else {
            nexmarkConf.maxEmitSpeed = true;   // fire as fast as possible
        }
        nexmarkConf.personProportion   = personProp;
        nexmarkConf.auctionProportion  = auctionProp;
        nexmarkConf.bidProportion      = bidProp;
        nexmarkConf.numActivePeople    = workingSet;
        nexmarkConf.hotSellersRatio    = hotSellers;
        nexmarkConf.avgPersonByteSize  = personAvgSize;
        nexmarkConf.compressibleExtra  = compressible;
        nexmarkConf.firstEventRate     = 10_000;
        nexmarkConf.nextEventRate      = 10_000;
        nexmarkConf.numEventGenerators = parallelism;

        GeneratorConfig generatorConfig = new GeneratorConfig(
                nexmarkConf,
                System.currentTimeMillis(),
                1,
                numEvents,
                -1L,
                1);

        TypeInformation<RowData> rowDataType = TypeInformation.of(RowData.class);
        NexmarkSource source = new NexmarkSource(generatorConfig, rowDataType);

        // -- Source ----------------------------------------------------------
        // In measure mode, use a different UID so the savepoint's source state
        // (containing the build-phase GeneratorConfig with limited maxEvents) is
        // NOT restored.  The source starts fresh with the CLI-provided config.
        DataStream<RowData> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Nexmark Source")
                .setParallelism(parallelism)
                .uid(measure ? "nexmark-source-measure" : "nexmark-source");

        // -- Extract Person (event_type == 0) -------------------------------
        // In --phase measure every Person event is dropped here, before keyBy /
        // processElement1, so personState stays frozen at the restored size.
        // RowData layout (RowDataEventDeserializer):
        //  [0] event_type (INT)
        //  [1] person ROW(8):   id, name, emailAddress, creditCard, city, state, dateTime, extra
        //  [2] auction ROW(10): id, itemName, description, initialBid, reserve, dateTime, expires, seller, category, extra
        //  [3] bid ROW(7):      auction, bidder, price, channel, url, dateTime, extra
        final boolean dropPersons = measure;
        DataStream<Person> persons = events
                .filter(row -> !dropPersons
                        && row.getInt(0) == Event.Type.PERSON.value
                        && !row.isNullAt(1))
                .map(row -> {
                    RowData p = row.getRow(1, 8);
                    return new Person(
                            p.getLong(0),
                            p.getString(1).toString(),
                            p.getString(2).toString(),
                            p.getString(3).toString(),
                            p.getString(4).toString(),
                            p.getString(5).toString(),
                            Instant.ofEpochMilli(p.getTimestamp(6, 3).getMillisecond()),
                            p.getString(7).toString());
                })
                .returns(TypeInformation.of(Person.class))
                .name("Extract Person")
                .uid("extract-person");

        // -- Extract Auction (event_type == 1) -----------------------------
        // Category filter REMOVED: every auction enters the join and probes personState.
        DataStream<Auction> auctions = events
                .filter(row -> row.getInt(0) == Event.Type.AUCTION.value && !row.isNullAt(2))
                .map(row -> {
                    RowData a = row.getRow(2, 10);
                    return new Auction(
                            a.getLong(0),
                            a.getString(1).toString(),
                            a.getString(2).toString(),
                            a.getLong(3),
                            a.getLong(4),
                            Instant.ofEpochMilli(a.getTimestamp(5, 3).getMillisecond()),
                            Instant.ofEpochMilli(a.getTimestamp(6, 3).getMillisecond()),
                            a.getLong(7),
                            a.getLong(8),
                            a.getString(9).toString());
                })
                .returns(TypeInformation.of(Auction.class))
                .name("Extract Auction")
                .uid("extract-auction");

        // -- Q3 Join (async KeyedCoProcessFunction) -------------------------
        // Both streams keyed by person.id / auction.seller; async state access.
        TypeInformation<Tuple2<Long, Long>> resultType =
                new TupleTypeInfo<>(Types.LONG, Types.LONG);

        SingleOutputStreamOperator<Tuple2<Long, Long>> result =
                persons.keyBy(p -> p.id)
                       .connect(auctions.keyBy(a -> a.seller))
                       .enableAsyncState()
                       .process(new Q3JoinFunction())
                       .returns(resultType)
                       .name("Q3 Join (person - auction)")
                       .uid("q3-join");

        // -- Sink: blackhole (discard) -------------------------------------
        result.sinkTo(new DiscardingSink<>())
              .name("Blackhole Sink")
              .uid("blackhole-sink");

        System.out.println("Submitting job to cluster...");
        env.execute("Nexmark Q3 Java  phase=" + (measure ? "measure" : "build")
                + "  ws=" + workingSet + "  events=" + numEvents);
    }

    // =========================================================================
    // Q3 Join Function - async state, clear-on-join
    // =========================================================================

    /**
     * Stateful streaming join for the Q3 experiment.
     *
     * <p>Key = person.id (stream-1) = auction.seller (stream-2).
     *
     * <p>State:
     * <ul>
     *   <li>{@code personState}  - the Person for this key (the "table" side; never cleared;
     *       frozen in the measure phase because Person events are dropped upstream).
     *   <li>{@code auctionState} - clear-on-join buffer: holds an auction only while its seller
     *       has not been seen yet; drained + cleared once the Person arrives; empty in the
     *       measure phase (every seller is already in personState).
     * </ul>
     */
    public static class Q3JoinFunction
            extends KeyedCoProcessFunction<Long, Person, Auction, Tuple2<Long, Long>> {

        private static final long serialVersionUID = 2L;

        private transient ValueState<Person> personState;
        private transient ListState<Auction> auctionState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            personState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("person", TypeInformation.of(Person.class)));
            auctionState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("auctions", TypeInformation.of(Auction.class)));
        }

        /**
         * Stream-1: Person arrives (build phase only).
         * Store it, drain any buffered auctions for this key, then clear the buffer.
         */
        @Override
        public void processElement1(Person person, Context ctx,
                                    Collector<Tuple2<Long, Long>> out) throws Exception {
            ThrowingConsumer<Auction, Exception> emit =
                    a -> out.collect(Tuple2.of(person.id, a.id));
            personState.asyncUpdate(person)
                    .thenCompose(v -> auctionState.asyncGet())
                    .thenCompose(it -> it == null
                            ? StateFutureUtils.completedVoidFuture()
                            : it.onNext(emit))
                    .thenAccept(v -> auctionState.asyncClear());
        }

        /**
         * Stream-2: Auction arrives.
         * If the seller is already known -> join now, emit result.
         * If not -> drop silently.  In measure mode personState is frozen, so
         * buffering would cause an unbounded write leak that pollutes the cache.
         */
        @Override
        public void processElement2(Auction auction, Context ctx,
                                    Collector<Tuple2<Long, Long>> out) throws Exception {
            personState.asyncValue().thenAccept(p -> {
                if (p != null) {
                    out.collect(Tuple2.of(p.id, auction.id));
                }
                // else: seller unknown — drop auction (no state write)
            });
        }       
    }
}
