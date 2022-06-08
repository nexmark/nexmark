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

package com.github.nexmark.flink.metric;

import com.github.nexmark.flink.metric.tps.TpsMetric;
import com.github.nexmark.flink.utils.NexmarkUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A HTTP client to request TPS metric to JobMaster REST API.
 */
public class FlinkRestClient {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkRestClient.class);

	private static int CONNECT_TIMEOUT = 5000;
	private static int SOCKET_TIMEOUT = 60000;
	private static int CONNECTION_REQUEST_TIMEOUT = 10000;
	private static int MAX_IDLE_TIME = 60000;
	private static int MAX_CONN_TOTAL = 60;
	private static int MAX_CONN_PER_ROUTE = 30;

	private final String jmEndpoint;
	private final CloseableHttpClient httpClient;
	private final Map<String, String> jobIds;
	private volatile String lastJobId;

	public FlinkRestClient(String jmAddress, int jmPort) {
		this.jmEndpoint = jmAddress + ":" + jmPort;

		RequestConfig requestConfig = RequestConfig.custom()
			.setSocketTimeout(SOCKET_TIMEOUT)
			.setConnectTimeout(CONNECT_TIMEOUT)
			.setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT)
			.build();
		PoolingHttpClientConnectionManager httpClientConnectionManager = new PoolingHttpClientConnectionManager();
		httpClientConnectionManager.setValidateAfterInactivity(MAX_IDLE_TIME);
		httpClientConnectionManager.setDefaultMaxPerRoute(MAX_CONN_PER_ROUTE);
		httpClientConnectionManager.setMaxTotal(MAX_CONN_TOTAL);

		this.httpClient = HttpClientBuilder.create()
			.setConnectionManager(httpClientConnectionManager)
			.setDefaultRequestConfig(requestConfig)
			.build();

		this.jobIds = new ConcurrentHashMap<>(50);
		this.lastJobId = "";
	}

	public synchronized void updateAllJobStatus() {
		String url = String.format("http://%s/jobs", jmEndpoint);
		String response = executeAsString(url);
		try {
			JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(response);
			JsonNode jobs = jsonNode.get("jobs");
			for (JsonNode job : jobs) {
				String id = job.get("id").asText();
				if (jobIds.put(id, job.get("status").asText()) == null) {
					lastJobId = id;
				}
			}
		} catch (JsonProcessingException e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
	}

	public void cancelJob(String jobId) {
		LOG.info("Stopping Job: {}", jobId);
		String url = String.format("http://%s/jobs/%s?mode=cancel", jmEndpoint, jobId);
		patch(url);
	}

	public String getCurrentJobId() {
		updateAllJobStatus();
		return lastJobId;
	}

	public boolean isJobRunning() {
		updateAllJobStatus();
		return !isNullOrEmpty(lastJobId) && jobIds.get(lastJobId).equalsIgnoreCase("RUNNING");
	}

	public boolean isJobCancellingOrFinished() {
		updateAllJobStatus();
		if (!isNullOrEmpty(lastJobId)) {
			String status = jobIds.get(lastJobId);
			return status.equalsIgnoreCase("CANCELLING") || status.equalsIgnoreCase("CANCELED") || status.equalsIgnoreCase("FINISHED");
		}
		return true;
	}

	private static boolean isNullOrEmpty(String string) {
		return string == null || string.length() == 0;
	}

	public String getSourceVertexId(String jobId) {
		String url = String.format("http://%s/jobs/%s", jmEndpoint, jobId);
		String response = executeAsString(url);
		try {
			JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(response);
			JsonNode vertices = jsonNode.get("vertices");
			JsonNode sourceVertex = vertices.get(0);
			checkArgument(
				sourceVertex.get("name").asText().startsWith("Source:"),
				"The first vertex is not a source.");
			return sourceVertex.get("id").asText();
		} catch (Exception e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
	}

	public String getTpsMetricName(String jobId, String vertexId) {
		String url = String.format("http://%s/jobs/%s/vertices/%s/subtasks/metrics", jmEndpoint, jobId, vertexId);
		String response = executeAsString(url);
		try {
			ArrayNode arrayNode = (ArrayNode) NexmarkUtils.MAPPER.readTree(response);
			for (JsonNode node : arrayNode) {
				String metricName = node.get("id").asText();
				if (metricName.startsWith("Source_") && metricName.endsWith(".numRecordsOutPerSecond")) {
					return metricName;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
		throw new RuntimeException("Can't find TPS metric name from the response:\n" + response);
	}

	public synchronized TpsMetric getTpsMetric(String jobId, String vertexId, String tpsMetricName) {
		String url = String.format(
			"http://%s/jobs/%s/vertices/%s/subtasks/metrics?get=%s",
			jmEndpoint,
			jobId,
			vertexId,
			tpsMetricName);
		String response = executeAsString(url);
		return TpsMetric.fromJson(response);
	}

	private void patch(String url) {
		HttpPatch httpPatch = new HttpPatch();
		httpPatch.setURI(URI.create(url));
		HttpResponse response;
		try {
			httpPatch.setHeader("Connection", "close");
			response = httpClient.execute(httpPatch);
			int httpCode = response.getStatusLine().getStatusCode();
			if (httpCode != HttpStatus.SC_ACCEPTED) {
				String msg = String.format("http execute failed,status code is %d", httpCode);
				throw new RuntimeException(msg);
			}
		} catch (Exception e) {
			httpPatch.abort();
			throw new RuntimeException(e);
		}
	}

	private String executeAsString(String url) {
		HttpGet httpGet = new HttpGet();
		httpGet.setURI(URI.create(url));
		try {
			HttpEntity entity = execute(httpGet).getEntity();
			if (entity != null) {
				return EntityUtils.toString(entity, Consts.UTF_8);
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to request URL " + url, e);
		}
		throw new RuntimeException(String.format("Response of URL %s is null.", url));
	}

	private HttpResponse execute(HttpRequestBase httpRequestBase) throws Exception {
		HttpResponse response;
		try {
			httpRequestBase.setHeader("Connection", "close");
			response = httpClient.execute(httpRequestBase);
			int httpCode = response.getStatusLine().getStatusCode();
			if (httpCode != HttpStatus.SC_OK) {
				String msg = String.format("http execute failed,status code is %d", httpCode);
				throw new RuntimeException(msg);
			}
			return response;
		} catch (Exception e) {
			httpRequestBase.abort();
			throw e;
		}
	}

	public synchronized void close() {
		try {
			if (httpClient != null) {
				httpClient.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
