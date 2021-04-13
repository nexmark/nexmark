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

package com.github.nexmark.flink.generator;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.github.nexmark.flink.model.Event;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class NexmarkGeneratorTest {

	@Parameterized.Parameter(0)
	public boolean useExtendedBidMode;

	@Parameterized.Parameters(name = "useExtendedBidMode = {0}")
	public static Object[] parameters() {
		return new Object[][] {
				new Object[] {true},
				new Object[] {false}
		};
	}

	@Test
	public void testGenerate() {
		NexmarkConfiguration nexmarkConfiguration = new NexmarkConfiguration();
		nexmarkConfiguration.bidProportion = 46;
		nexmarkConfiguration.extendedBidMode = useExtendedBidMode;
		nexmarkConfiguration.numCategories = 10_000;
		GeneratorConfig generatorConfig = new GeneratorConfig(
			nexmarkConfiguration, 0, 15000, 0);
		NexmarkGenerator generator = new NexmarkGenerator(generatorConfig);
		int count = 0;
		while (generator.hasNext()) {
			Event event = generator.next().getValue();
			count ++;
			System.out.println(event);
		}
		System.out.println("Total event:" + count);
	}
}
