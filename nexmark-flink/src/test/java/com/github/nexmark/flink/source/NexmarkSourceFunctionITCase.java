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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.github.nexmark.flink.generator.GeneratorConfig;
import com.github.nexmark.flink.model.Event;
import org.junit.Test;

public class NexmarkSourceFunctionITCase {

	@Test
	public void testDataStreamSource() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);
		NexmarkConfiguration nexmarkConfiguration = new NexmarkConfiguration();
		nexmarkConfiguration.bidProportion = 46;
		GeneratorConfig generatorConfig = new GeneratorConfig(
			nexmarkConfiguration, System.currentTimeMillis(), 1, 100, 1);
		env.addSource(new NexmarkSourceFunction<>(
				generatorConfig,
				(EventDeserializer<String>) Event::toString,
				BasicTypeInfo.STRING_TYPE_INFO))
			.print();

		env.execute();
	}
}
