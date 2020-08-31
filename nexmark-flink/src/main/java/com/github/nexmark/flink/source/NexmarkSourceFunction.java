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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Preconditions;

import com.github.nexmark.flink.generator.NexmarkGenerator;
import com.github.nexmark.flink.generator.GeneratorConfig;

import java.util.ArrayList;
import java.util.List;

public class NexmarkSourceFunction<T>
		extends RichParallelSourceFunction<T>
		implements CheckpointedFunction, ResultTypeQueryable<T> {

	/** Configuration for generator to use when reading synthetic events. May be split. */
	private final GeneratorConfig config;

	private final EventDeserializer<T> deserializer;

	private final TypeInformation<T> resultType;

	private transient NexmarkGenerator generator;

	/** The number of elements emitted already. */
	private volatile long numElementsEmitted;

	/** Flag to make the source cancelable. */
	private volatile boolean isRunning = true;

	private transient ListState<Long> checkpointedState;

	public NexmarkSourceFunction(GeneratorConfig config, EventDeserializer<T> deserializer, TypeInformation<T> resultType) {
		this.config = config;
		this.deserializer = deserializer;
		this.resultType = resultType;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.generator = new NexmarkGenerator(getSubGeneratorConfig());
	}

	private GeneratorConfig getSubGeneratorConfig() {
		int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
		int taskId = getRuntimeContext().getIndexOfThisSubtask();
		return config.split(parallelism).get(taskId);
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		Preconditions.checkState(this.checkpointedState == null,
			"The " + getClass().getSimpleName() + " has already been initialized.");

		this.checkpointedState = context.getOperatorStateStore().getListState(
			new ListStateDescriptor<>(
				"elements-count-state",
				LongSerializer.INSTANCE
			)
		);

		if (context.isRestored()) {
			List<Long> retrievedStates = new ArrayList<>();
			for (Long entry : this.checkpointedState.get()) {
				retrievedStates.add(entry);
			}

			// given that the parallelism of the function is 1, we can only have 1 state
			Preconditions.checkArgument(retrievedStates.size() == 1,
				getClass().getSimpleName() + " retrieved invalid state.");

			long numElementsToSkip = retrievedStates.get(0);
			this.generator = new NexmarkGenerator(getSubGeneratorConfig(), numElementsToSkip, 0);
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
		Preconditions.checkState(this.checkpointedState != null,
			"The " + getClass().getSimpleName() + " has not been properly initialized.");

		this.checkpointedState.clear();
		this.checkpointedState.add(numElementsEmitted);
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		while (isRunning && generator.hasNext()) {
			long now = System.currentTimeMillis();
			NexmarkGenerator.NextEvent nextEvent = generator.nextEvent();

			if (nextEvent.wallclockTimestamp > now) {
				// sleep until wall clock less than current timestamp.
				Thread.sleep(nextEvent.wallclockTimestamp - now);
			}

			T next = deserializer.deserialize(nextEvent.event);
			synchronized (ctx.getCheckpointLock()) {
				numElementsEmitted = generator.getEventsCountSoFar();
				try {
					ctx.collect(next);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}


	@Override
	public void close() throws Exception {
		super.close();
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return resultType;
	}
}
