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
package com.github.nexmark.flink.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * User defined function tou count number of specified characters.
 */
public class CountChar extends ScalarFunction {

	public long eval(@DataTypeHint("STRING") StringData s, @DataTypeHint("STRING") StringData character) {
		long count = 0;
		if (null != s) {
			byte[] bytes = s.toBytes();
			byte chr = character.toBytes()[0];
			for (byte aByte : bytes) {
				if (aByte == chr) {
					count++;
				}
			}
		}
		return count;
	}
}
