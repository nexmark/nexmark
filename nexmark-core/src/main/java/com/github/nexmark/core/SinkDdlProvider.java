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

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides sink DDL configuration for Nexmark queries. Reads from a YAML file using SnakeYAML. Can
 * be used by any engine (Flink, Spark, etc.).
 *
 * <p>YAML format:
 *
 * <pre>
 * nexmark.sink.ddl.q0:
 *   connector: blackhole
 *
 * nexmark.sink.ddl.q10:
 *   connector: filesystem
 *   path: file://...
 *   format: csv
 * </pre>
 *
 * <p>Config resolution: user config overrides defaults from nexmark-sink-defaults.yaml
 */
public class SinkDdlProvider {

    /** Config key prefix for sink DDL. */
    public static final String SINK_DDL_KEY_PREFIX = "nexmark.sink.ddl";

    /** Resource path for default sink configurations. */
    public static final String DEFAULTS_RESOURCE = "nexmark-sink-defaults.yaml";

    /** Map from query key (e.g., "nexmark.sink.ddl.q0") to DDL string */
    private final Map<String, String> queryDdlMap;

    private SinkDdlProvider(Map<String, String> queryDdlMap) {
        this.queryDdlMap = queryDdlMap;
    }

    /**
     * Get the config key for a specific query.
     *
     * @param queryName the query name (e.g., "q0", "q10")
     * @return the config key (e.g., "nexmark.sink.ddl.q0")
     */
    public static String getConfigKey(String queryName) {
        return SINK_DDL_KEY_PREFIX + "." + queryName;
    }

    /**
     * Create provider from a YAML config file, merged with defaults.
     *
     * <p>Resolves ${NEXMARK_DIR} and ${SUBMIT_TIME} placeholders in default configurations.
     * User-provided configurations are used as-is.
     *
     * @param yamlPath path to nexmark.yaml
     * @param nexmarkDir value to substitute for ${NEXMARK_DIR}
     * @param submitTime value to substitute for ${SUBMIT_TIME}
     * @return configured provider
     */
    public static SinkDdlProvider fromYaml(Path yamlPath, String nexmarkDir, String submitTime)
            throws IOException {
        Map<String, Map<String, Object>> defaults = loadDefaults();
        Map<String, Map<String, Object>> userConfig = parseYamlFile(yamlPath);

        // Merge: user config overrides defaults
        Map<String, String> merged = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : defaults.entrySet()) {
            merged.put(
                    entry.getKey(),
                    resolveVariables(toDdlString(entry.getValue()), nexmarkDir, submitTime));
        }
        for (Map.Entry<String, Map<String, Object>> entry : userConfig.entrySet()) {
            merged.put(entry.getKey(), toDdlString(entry.getValue()));
        }

        return new SinkDdlProvider(merged);
    }

    /**
     * Create provider from YAML file at NEXMARK_CONF_DIR/nexmark.yaml. Falls back to defaults only
     * if env var not set or file not found.
     *
     * <p>Resolves ${NEXMARK_DIR} and ${SUBMIT_TIME} placeholders in default configurations.
     * User-provided configurations are used as-is.
     * 
     * TODO: consolidate with NexmarkGlobalConfiguration
     *
     * @param nexmarkDir value to substitute for ${NEXMARK_DIR}
     * @param submitTime value to substitute for ${SUBMIT_TIME}
     */
    public static SinkDdlProvider fromEnv(String nexmarkDir, String submitTime) {
        String confDir = System.getenv("NEXMARK_CONF_DIR");
        if (confDir != null) {
            Path yamlPath = Path.of(confDir, "nexmark.yaml");
            if (Files.exists(yamlPath)) {
                try {
                    return fromYaml(yamlPath, nexmarkDir, submitTime);
                } catch (IOException e) {
                    // Fall through to defaults only
                }
            }
        }
        // Use defaults only
        Map<String, Map<String, Object>> defaults = loadDefaults();
        Map<String, String> ddlMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : defaults.entrySet()) {
            ddlMap.put(
                    entry.getKey(),
                    resolveVariables(toDdlString(entry.getValue()), nexmarkDir, submitTime));
        }
        return new SinkDdlProvider(ddlMap);
    }

    /**
     * Create provider from a pre-built config map (for Flink integration). Keys like
     * "nexmark.sink.ddl.q0.connector" -> nested structure.
     *
     * <p>Resolves ${NEXMARK_DIR} and ${SUBMIT_TIME} placeholders in default configurations.
     * User-provided configurations are used as-is.
     *
     * @param flatConfig flat config map from Flink Configuration
     * @param nexmarkDir value to substitute for ${NEXMARK_DIR}
     * @param submitTime value to substitute for ${SUBMIT_TIME}
     */
    public static SinkDdlProvider fromMap(
            Map<String, String> flatConfig, String nexmarkDir, String submitTime) {
        Map<String, Map<String, Object>> defaults = loadDefaults();
        Map<String, Map<String, Object>> userConfig = parseFlatConfig(flatConfig);

        Map<String, String> merged = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : defaults.entrySet()) {
            merged.put(
                    entry.getKey(),
                    resolveVariables(toDdlString(entry.getValue()), nexmarkDir, submitTime));
        }
        for (Map.Entry<String, Map<String, Object>> entry : userConfig.entrySet()) {
            merged.put(entry.getKey(), toDdlString(entry.getValue()));
        }

        return new SinkDdlProvider(merged);
    }

    /**
     * Get sink DDL for a query.
     *
     * @param queryName the query name (e.g., "q0", "q10")
     * @return the DDL string for the WITH clause
     */
    public String getSinkDdl(String queryName) {
        String queryKey = getConfigKey(queryName);
        return queryDdlMap.getOrDefault(queryKey, "'connector' = 'blackhole'");
    }

    /**
     * Convert key-value map to DDL string format. E.g., {connector: filesystem, path: /foo} ->
     * "'connector' = 'filesystem', 'path' = '/foo'"
     */
    private static String toDdlString(Map<String, Object> properties) {
        return properties.entrySet().stream()
                .map(e -> "'" + e.getKey() + "' = '" + e.getValue() + "'")
                .collect(Collectors.joining(",\n  "));
    }

    /** Resolve ${NEXMARK_DIR} and ${SUBMIT_TIME} placeholders in a DDL string. */
    private static String resolveVariables(String ddl, String nexmarkDir, String submitTime) {
        return ddl.replace("${NEXMARK_DIR}", nexmarkDir).replace("${SUBMIT_TIME}", submitTime);
    }

    /** Load default sink configurations from classpath resource. */
    private static Map<String, Map<String, Object>> loadDefaults() {
        try (InputStream is =
                SinkDdlProvider.class.getClassLoader().getResourceAsStream(DEFAULTS_RESOURCE)) {
            if (is != null) {
                return parseYamlStream(is);
            }
        } catch (IOException e) {
            // Use empty defaults
        }
        return new HashMap<>();
    }

    private static Map<String, Map<String, Object>> parseYamlFile(Path yamlPath)
            throws IOException {
        try (InputStream is = Files.newInputStream(yamlPath)) {
            return parseYamlStream(is);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, Object>> parseYamlStream(InputStream is) {
        Yaml yaml = new Yaml();
        Map<String, Object> rawConfig = yaml.load(is);
        if (rawConfig == null) {
            return new HashMap<>();
        }

        // Filter and convert to expected structure
        Map<String, Map<String, Object>> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : rawConfig.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(SINK_DDL_KEY_PREFIX + ".") && entry.getValue() instanceof Map) {
                result.put(key, (Map<String, Object>) entry.getValue());
            }
        }
        return result;
    }

    /**
     * Parse flat config map (from Flink Configuration.toMap()). Keys like
     * "nexmark.sink.ddl.q0.connector" -> nested structure.
     */
    private static Map<String, Map<String, Object>> parseFlatConfig(
            Map<String, String> flatConfig) {
        Map<String, Map<String, Object>> result = new HashMap<>();

        for (Map.Entry<String, String> entry : flatConfig.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(SINK_DDL_KEY_PREFIX + ".")) {
                continue;
            }

            // Parse: nexmark.sink.ddl.q0.connector -> queryKey=nexmark.sink.ddl.q0, prop=connector
            String afterPrefix = key.substring(SINK_DDL_KEY_PREFIX.length() + 1);
            int dotIdx = afterPrefix.indexOf(".");
            if (dotIdx > 0) {
                String queryName = afterPrefix.substring(0, dotIdx);
                String propName = afterPrefix.substring(dotIdx + 1);
                String queryKey = SINK_DDL_KEY_PREFIX + "." + queryName;

                result.computeIfAbsent(queryKey, k -> new LinkedHashMap<>())
                        .put(propName, entry.getValue());
            }
        }

        return result;
    }
}
