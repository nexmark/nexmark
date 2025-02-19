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

import com.github.nexmark.flink.generator.GeneratorConfig;
import com.github.nexmark.flink.generator.NexmarkGenerator;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class NexmarkSourceReader implements SourceReader<RowData, NexmarkSource.NexmarkSourceSplit> {

    private final SourceReaderContext context;
    private final EventDeserializer<RowData> deserializer;
    private final Counter numRecordsInCounter;
    private final boolean isKeepAlive;
    private final GeneratorConfig config;
    private NexmarkSource.NexmarkSourceSplit sourceSplit;
    private NexmarkGenerator generator;

    NexmarkSourceReader(SourceReaderContext sourceReaderContext,
                        GeneratorConfig config,
                        EventDeserializer<RowData> deserializer) {
        this.context = sourceReaderContext;
        this.isKeepAlive = config.isSourceKeepAlive();
        this.config = config;
        this.deserializer = deserializer;
        this.numRecordsInCounter = context.metricGroup().getIOMetricGroup().getNumRecordsInCounter();
    }

    @Override
    public void start() {
        if (sourceSplit == null) {
            context.sendSplitRequest();
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> readerOutput) throws Exception {
        if (sourceSplit == null || generator == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }
        if (!generator.hasNext()) {
            return isKeepAlive ? InputStatus.NOTHING_AVAILABLE : InputStatus.END_OF_INPUT;
        }
        long now = System.currentTimeMillis();
        NexmarkGenerator.NextEvent nextEvent = generator.next();
        if (nextEvent.wallclockTimestamp > now) {
            Thread.sleep(nextEvent.wallclockTimestamp - now);
        }
        readerOutput.collect(deserializer.deserialize(nextEvent.event));
        numRecordsInCounter.inc();
        return InputStatus.MORE_AVAILABLE;
    }

    @Override
    public List<NexmarkSource.NexmarkSourceSplit> snapshotState(long l) {
        sourceSplit.setNumEmittedSoFar(generator.getEventsCountSoFar());
        sourceSplit.setWallClockBaseTime(generator.getWallclockBaseTime());
        return Collections.singletonList(sourceSplit);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<NexmarkSource.NexmarkSourceSplit> list) {
        Preconditions.checkState(list.size() == 1, "Only one split supported for one reader");
        Preconditions.checkState(sourceSplit == null, "We already have one split.");
        sourceSplit = list.get(0);
        generator = new NexmarkGenerator(sourceSplit.getGeneratorConfig().reconfigure(config, config.isSourceIgnoreStop()), sourceSplit.getNumEmittedSoFar(), sourceSplit.getWallClockBaseTime());
    }

    @Override
    public void notifyNoMoreSplits() {
    }

    @Override
    public void close() throws Exception {
    }
}
