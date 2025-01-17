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

 package com.github.nexmark.standalone.source;

 import com.github.nexmark.flink.NexmarkConfiguration;
 import com.github.nexmark.flink.generator.GeneratorConfig;
 import com.github.nexmark.flink.model.Event;
 
 import org.junit.Test;
 
 public class NexmarkSourceFunctionITCase {
 
     /** This is used to test the standalone Java data generator.
      * Its parameters can be adjusted as necessary
      */
     @Test
     public void testDataStream() throws Exception {
         // Creating nexmark configuration for the data stream
         NexmarkConfiguration nexmarkConfig = new NexmarkConfiguration();
 
         // Populating all of the nexmark configuration settings
         nexmarkConfig.auctionProportion = 20;
         nexmarkConfig.bidProportion = 46;
         nexmarkConfig.personProportion = 30;
 
         // Creating a generator configuration for the data stream 
         // Can also be changed as necessary
         GeneratorConfig generatorConfig = new GeneratorConfig(
             nexmarkConfig, 
             System.currentTimeMillis(), 
             1, 
             1000, 
             1
         );
 
         // Opening the NexmarkSourceFunction, which uses the event deserializer to get a toString() version of it
         NexmarkSourceFunction nexFunc = new NexmarkSourceFunction<>(generatorConfig, (EventDeserializer<String>) Event::toString);
         nexFunc.open();
         SourceContext context = new SourceContext<>();
 
         // Used to track the events and write to the JSON file - please see class for details on implementation
         nexFunc.run(context);
     }
 }
 