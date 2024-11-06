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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * Nexmark source, using Source V2 of Flink.
 */
public class NexmarkSource implements Source<RowData,
        NexmarkSource.NexmarkSourceSplit,
        Collection<NexmarkSource.NexmarkSourceSplit>>,
        ResultTypeQueryable<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(NexmarkSource.class);

    private final GeneratorConfig config;
    private final TypeInformation<RowData> outputType;
    private final RowDataEventDeserializer deserializer;

    NexmarkSource(GeneratorConfig config, TypeInformation<RowData> outputType) {
        this.config = config;
        this.outputType = outputType;
        this.deserializer = new RowDataEventDeserializer();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<NexmarkSourceSplit, Collection<NexmarkSourceSplit>> createEnumerator(
            SplitEnumeratorContext<NexmarkSourceSplit> splitEnumeratorContext) throws Exception {
        LOG.info("Creating Nexmark Enumerator");
        return new StaticSplitEnumerator(splitEnumeratorContext, getSplits(splitEnumeratorContext.currentParallelism()));
    }

    @Override
    public SplitEnumerator<NexmarkSourceSplit, Collection<NexmarkSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<NexmarkSourceSplit> splitEnumeratorContext,
            Collection<NexmarkSourceSplit> nexmarkSourceSplits) throws Exception {
        if (nexmarkSourceSplits.size() != splitEnumeratorContext.currentParallelism()) {
            throw new UnsupportedOperationException("We don't support rescale the source");
        }
        return new StaticSplitEnumerator(splitEnumeratorContext, nexmarkSourceSplits);
    }

    @Override
    public SimpleVersionedSerializer<NexmarkSourceSplit> getSplitSerializer() {
        return SimpleSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<Collection<NexmarkSourceSplit>> getEnumeratorCheckpointSerializer() {
        return new SimpleSplitCollectionSerializer(SimpleSplitSerializer.INSTANCE);
    }

    @Override
    public SourceReader<RowData, NexmarkSourceSplit> createReader(SourceReaderContext sourceReaderContext) {
        LOG.info("Creating Nexmark Reader");
        return new NexmarkSourceReader(sourceReaderContext, deserializer);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return outputType;
    }

    List<NexmarkSourceSplit> getSplits(int parallelism) {
        List<GeneratorConfig> subConfigs = config.split(parallelism);
        List<NexmarkSourceSplit> splits = new ArrayList<>(parallelism);
        for (GeneratorConfig subConfig : subConfigs) {
            splits.add(new NexmarkSourceSplit(UUID.randomUUID().toString(), subConfig));
        }
        return splits;
    }

    public static class StaticSplitEnumerator
            implements SplitEnumerator<NexmarkSourceSplit, Collection<NexmarkSourceSplit>> {

        private static final Logger LOG = LoggerFactory.getLogger(StaticSplitEnumerator.class);

        private final SplitEnumeratorContext<NexmarkSourceSplit> context;
        private final LinkedList<NexmarkSourceSplit> splits;

        StaticSplitEnumerator(SplitEnumeratorContext<NexmarkSourceSplit> context,
                              Collection<NexmarkSourceSplit> splits) {
            this.context = context;
            this.splits = new LinkedList<>(splits);
            LOG.info("StaticSplitEnumerator init with {} splits", splits.size());
        }

        @Override
        public void start() {
        }

        @Override
        public void handleSplitRequest(int subtask, @Nullable String hostname) {
            if (!context.registeredReaders().containsKey(subtask)) {
                // reader failed between sending the request and now. skip this request.
                return;
            }

            final NexmarkSourceSplit split = splits.removeFirst();
            if (split != null) {
                context.assignSplit(split, subtask);
                LOG.info("Assigned split to subtask {} : {}", subtask, split);
            } else {
                context.signalNoMoreSplits(subtask);
                LOG.info("No more splits available for subtask {}", subtask);
            }
        }

        @Override
        public void addSplitsBack(List<NexmarkSourceSplit> list, int i) {
            splits.addAll(list);
        }

        @Override
        public void addReader(int i) {
        }

        @Override
        public Collection<NexmarkSourceSplit> snapshotState(long l) throws Exception {
            return new ArrayList<>(splits);
        }

        @Override
        public void close() throws IOException {
        }
    }

    public static class NexmarkSourceSplit implements SourceSplit, Serializable {

        private final String id;
        private final GeneratorConfig generatorConfig;
        private volatile long numEmittedSoFar;

        NexmarkSourceSplit(String id, GeneratorConfig generatorConfig) {
            this.id = id;
            this.generatorConfig = generatorConfig;
            this.numEmittedSoFar = 0;
        }

        @Override
        public String splitId() {
            return id;
        }

        public GeneratorConfig getGeneratorConfig() {
            return generatorConfig;
        }

        public long getNumEmittedSoFar() {
            return numEmittedSoFar;
        }

        public void setNumEmittedSoFar(long numEmittedSoFar) {
            this.numEmittedSoFar = numEmittedSoFar;
        }
    }

    public static class SimpleSplitSerializer implements SimpleVersionedSerializer<NexmarkSourceSplit> {

        public static SimpleSplitSerializer INSTANCE = new SimpleSplitSerializer();

        private SimpleSplitSerializer() {
        }

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(NexmarkSourceSplit nexmarkSourceSplit) throws IOException {
            try (ByteArrayOutputStream output = new ByteArrayOutputStream(64);
                 ObjectOutputStream oos = new ObjectOutputStream(output)) {
                oos.writeObject(nexmarkSourceSplit);
                return output.toByteArray();
            }
        }

        @Override
        public NexmarkSourceSplit deserialize(int i, byte[] bytes) throws IOException {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                 ObjectInputStream ois = new ObjectInputStream(bais)) {
                return (NexmarkSourceSplit) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class SimpleSplitCollectionSerializer
            implements SimpleVersionedSerializer<Collection<NexmarkSourceSplit>> {

        SimpleVersionedSerializer<NexmarkSourceSplit> splitSerializer;

        public SimpleSplitCollectionSerializer(SimpleVersionedSerializer<NexmarkSourceSplit> splitSerializer) {
            this.splitSerializer = splitSerializer;
        }

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Collection<NexmarkSourceSplit> splits) throws IOException {
            final ArrayList<byte[]> serializedSplits = new ArrayList<>(splits.size());
            int totalLen = 4;
            for (NexmarkSourceSplit split : splits) {
                final byte[] serSplit = splitSerializer.serialize(split);
                serializedSplits.add(serSplit);
                totalLen += serSplit.length + 4; // 4 bytes for the length field
            }

            final byte[] result = new byte[totalLen];
            final ByteBuffer byteBuffer = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
            byteBuffer.putInt(splits.size());
            for (byte[] splitBytes : serializedSplits) {
                byteBuffer.putInt(splitBytes.length);
                byteBuffer.put(splitBytes);
            }
            return result;
        }

        @Override
        public Collection<NexmarkSourceSplit> deserialize(int i, byte[] input) throws IOException {
            final ByteBuffer bb = ByteBuffer.wrap(input).order(ByteOrder.LITTLE_ENDIAN);

            final int splitSerializerVersion = bb.getInt();
            final int numSplits = bb.getInt();
            final int numPaths = bb.getInt();

            final ArrayList<NexmarkSourceSplit> splits = new ArrayList<>(numSplits);

            for (int remaining = numSplits; remaining > 0; remaining--) {
                final byte[] bytes = new byte[bb.getInt()];
                bb.get(bytes);
                final NexmarkSourceSplit split = splitSerializer.deserialize(splitSerializerVersion, bytes);
                splits.add(split);
            }
            return splits;
        }
    }
}
