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

package com.github.nexmark.core;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link SinkDdlProvider}. */
class SinkDdlProviderTest {

    private static final String TEST_NEXMARK_DIR = "/opt/nexmark";
    private static final String TEST_SUBMIT_TIME = "2024-01-15T10:30:00";
    private static final List<String> BLACKHOLE_QUERIES =
            Arrays.asList(
                    "q0", "q1", "q2", "q3", "q4", "q5", "q7", "q8", "q9", "q11", "q12", "q13",
                    "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22", "q23");

    @Test
    void testFromEnvLoadsDefaults() {
        SinkDdlProvider provider = SinkDdlProvider.fromEnv(TEST_NEXMARK_DIR, TEST_SUBMIT_TIME);

        // Verify blackhole queries
        for (String query : BLACKHOLE_QUERIES) {
            String ddl = provider.getSinkDdl(query);
            assertEquals(
                    "'connector' = 'blackhole'",
                    ddl,
                    "Query " + query + " should have blackhole connector");
        }

        // Verify q10 filesystem with resolved variables
        String q10Ddl = provider.getSinkDdl("q10");
        assertTrue(q10Ddl.contains("'connector' = 'filesystem'"));
        assertTrue(
                q10Ddl.contains(
                        "'path' = 'file://"
                                + TEST_NEXMARK_DIR
                                + "/data/output/"
                                + TEST_SUBMIT_TIME
                                + "/bid/'"));
        assertFalse(q10Ddl.contains("${NEXMARK_DIR}"));
        assertFalse(q10Ddl.contains("${SUBMIT_TIME}"));

        // Verify unknown query fallback
        assertEquals("'connector' = 'blackhole'", provider.getSinkDdl("unknown_query"));
    }

    @Test
    void testFromYamlOverridesDefaults() throws IOException {
        Path tempYaml = Files.createTempFile("nexmark-test", ".yaml");
        try {
            String yamlContent =
                    "nexmark.sink.ddl.q0:\n"
                            + "  connector: kafka\n"
                            + "  topic: nexmark-q0\n"
                            + "\n"
                            + "nexmark.sink.ddl.q10:\n"
                            + "  connector: hudi\n"
                            + "  path: s3a://bucket/q10\n";
            Files.writeString(tempYaml, yamlContent);

            SinkDdlProvider provider =
                    SinkDdlProvider.fromYaml(tempYaml, TEST_NEXMARK_DIR, TEST_SUBMIT_TIME);

            // q0 overridden
            assertTrue(provider.getSinkDdl("q0").contains("'connector' = 'kafka'"));

            // q10 overridden (no longer filesystem)
            String q10Ddl = provider.getSinkDdl("q10");
            assertTrue(q10Ddl.contains("'connector' = 'hudi'"));
            assertFalse(q10Ddl.contains("'connector' = 'filesystem'"));

            // q1 still default
            assertEquals("'connector' = 'blackhole'", provider.getSinkDdl("q1"));
        } finally {
            Files.deleteIfExists(tempYaml);
        }
    }

    @Test
    void testFromMapOverridesDefaults() {
        Map<String, String> flatConfig = new HashMap<>();
        flatConfig.put("nexmark.sink.ddl.q0.connector", "iceberg");
        flatConfig.put("nexmark.sink.ddl.q0.catalog-name", "hive_catalog");
        flatConfig.put("some.other.config", "ignored");

        SinkDdlProvider provider =
                SinkDdlProvider.fromMap(flatConfig, TEST_NEXMARK_DIR, TEST_SUBMIT_TIME);

        // q0 overridden
        String q0Ddl = provider.getSinkDdl("q0");
        assertTrue(q0Ddl.contains("'connector' = 'iceberg'"));
        assertTrue(q0Ddl.contains("'catalog-name' = 'hive_catalog'"));

        // q1 still default
        assertEquals("'connector' = 'blackhole'", provider.getSinkDdl("q1"));
    }

    @Test
    void testGetConfigKey() {
        assertEquals("nexmark.sink.ddl.q0", SinkDdlProvider.getConfigKey("q0"));
        assertEquals("nexmark.sink.ddl.q10", SinkDdlProvider.getConfigKey("q10"));
    }

    @Test
    void testEmptyConfigUsesDefaults() throws IOException {
        // Empty YAML
        Path tempYaml = Files.createTempFile("nexmark-test", ".yaml");
        try {
            Files.writeString(tempYaml, "# Empty\n");
            SinkDdlProvider yamlProvider =
                    SinkDdlProvider.fromYaml(tempYaml, TEST_NEXMARK_DIR, TEST_SUBMIT_TIME);
            assertEquals("'connector' = 'blackhole'", yamlProvider.getSinkDdl("q0"));
        } finally {
            Files.deleteIfExists(tempYaml);
        }

        // Empty Map
        SinkDdlProvider mapProvider =
                SinkDdlProvider.fromMap(new HashMap<>(), TEST_NEXMARK_DIR, TEST_SUBMIT_TIME);
        assertEquals("'connector' = 'blackhole'", mapProvider.getSinkDdl("q0"));
        assertTrue(mapProvider.getSinkDdl("q10").contains("'connector' = 'filesystem'"));
    }
}
