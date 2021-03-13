/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.exporter.provider.http;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import okhttp3.*;
import com.google.gson.Gson;
import org.apache.skywalking.apm.commons.datacarrier.DataCarrier;
import org.apache.skywalking.apm.commons.datacarrier.consumer.IConsumer;
import org.apache.skywalking.oap.server.core.analysis.metrics.DoubleValueHolder;
import org.apache.skywalking.oap.server.core.analysis.metrics.IntValueHolder;
import org.apache.skywalking.oap.server.core.analysis.metrics.LongValueHolder;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.analysis.metrics.MetricsMetaInfo;
import org.apache.skywalking.oap.server.core.analysis.metrics.MultiIntValuesHolder;
import org.apache.skywalking.oap.server.core.analysis.metrics.WithMetadata;
import org.apache.skywalking.oap.server.core.exporter.ExportData;
import org.apache.skywalking.oap.server.core.exporter.ExportEvent;
import org.apache.skywalking.oap.server.core.exporter.MetricValuesExportService;
import org.apache.skywalking.oap.server.exporter.provider.MetricFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpExporter extends MetricFormatter implements MetricValuesExportService, IConsumer<ExportData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpExporter.class);
    private HttpExporterSetting setting;
    private final DataCarrier exportBuffer;
    private final Set<String> subscriptionSet;

    private final OkHttpClient httpClient;
    private static final MediaType jsonMediaType = MediaType.parse("application/json; charset=utf-8");

    private int waitCount = 0;
    private List<ExportData> dataBuffer;


    public HttpExporter(HttpExporterSetting setting) {
        this.setting = setting;
        exportBuffer = new DataCarrier<ExportData>(setting.getBufferChannelNum(), setting.getBufferChannelSize());
        exportBuffer.consume(this, 1, 1000);
        subscriptionSet = new HashSet<>();

        httpClient = new OkHttpClient.Builder().writeTimeout(1, TimeUnit.MINUTES).readTimeout(1, TimeUnit.MINUTES).build();
    }

    @Override
    public void export(ExportEvent event) {
        if (ExportEvent.EventType.TOTAL == event.getType()) {
            Metrics metrics = event.getMetrics();
            if (metrics instanceof WithMetadata) {
                MetricsMetaInfo meta = ((WithMetadata) metrics).getMeta();
                exportBuffer.produce(new ExportData(meta, metrics));
            }
        }
    }

    public void initSubscriptionList() {
        LOGGER.debug("Get exporter subscription list, {}", subscriptionSet);
    }

    @Override
    public void init() {

    }

    @Override
    public void consume(List<ExportData> data) {
        // 积累更多的数据，合并发送
        waitCount += 1;
        if (data.size() > 0) {
            if (dataBuffer == null) {
                dataBuffer = data;
            } else {
                dataBuffer.addAll(data);
            }
        }

        // 最多积累5次, 或者数据超过128个 发送一次
        if ((waitCount >= 5) || (dataBuffer != null && dataBuffer.size() > 128)) {
            waitCount = 0;
            if (dataBuffer != null && dataBuffer.size() > 0) {
                send(dataBuffer);
            }
            dataBuffer = null;
        }
    }

    @Override
    public void onError(List<ExportData> data, Throwable t) {
        LOGGER.error(t.getMessage(), t);
    }

    @Override
    public void onExit() {

    }

    private void send(List<ExportData> data) {
        int j = 0;
        final int batchCount = 1024;
        List<CmpMetric> ms = new ArrayList<CmpMetric>();
        for (int i = 0; i < data.size(); i++) {
            CmpMetric m = toCmpMetric(data.get(i));
            if (m == null) {
                continue;
            }

            ms.add(m);
            j += 1;
            if (j >= batchCount) {
                doSend(ms);
                j = 0;
                ms.clear();
            }
        }

        if (ms.size() > 0) {
            doSend(ms);
        }
    }

    private void doSend(List<CmpMetric> data) {
        long begin = System.currentTimeMillis();
        try {
            Gson gson = new Gson();
            String jsonStr = gson.toJson(data);
            Request request = new Request.Builder()
                    .url(setting.getCmpGateWay())
                    .post(RequestBody.create(jsonMediaType, jsonStr))
                    .build();

            httpClient.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    LOGGER.error("Post Metrics onFailure: " + e.getMessage());
                }

                @Override
                public void onResponse(Call call, Response response) throws IOException {
                    if (response.code() != 200) {
                        LOGGER.error("Post Metrics response: code:{}, msg:{}", response.code(), response.message());
                        return;
                    }

                    String restpStr = response.body().string();
                    if (restpStr.length() > 256) {
                        LOGGER.error("Post Metrics error:  ret {}", restpStr);
                        LOGGER.error("Post Metrics error:  data {}", jsonStr);
                        return;
                    }

                    MetricPostResp ret = gson.fromJson(restpStr, MetricPostResp.class);
                    if (ret != null && ( ret.code == 0)) {
                        LOGGER.error("Post Metrics error:  ret {}", restpStr);
                    }
                    LOGGER.info("Post Metrics size: {}, time:{}ms", data.size(), System.currentTimeMillis() - begin);
                }
            });
        } catch (Exception e) {
            LOGGER.error("Post Metrics Exception: " + e.getMessage());
        }
    }

    private CmpMetric toCmpMetric(ExportData d) {
        Metrics metrics = d.getMetrics();
        CmpMetric cm = new CmpMetric();
        if (metrics instanceof LongValueHolder) {
            long value = ((LongValueHolder) metrics).getValue();
            cm.value = (double) value;
        } else if (metrics instanceof IntValueHolder) {
            long value = ((IntValueHolder) metrics).getValue();
            cm.value = (double) value;
        } else if (metrics instanceof DoubleValueHolder) {
            cm.value = ((DoubleValueHolder) metrics).getValue();

        } else if (metrics instanceof MultiIntValuesHolder) {
            return null;
        } else {
            return null;
        }

        MetricsMetaInfo meta = d.getMeta();

        cm.metric = meta.getMetricsName();
        String entityName = getEntityName(meta);
        if (entityName == null) {
            return null;
        }
        cm.tags = new String[1][];
        cm.tags[0] = new String[]{"name", entityName};
        return cm;
    }
}

class CmpMetric {
    public String metric;
    public String[][] tags;
    public double value;
}


class MetricPostResp {
    public int code;
    public String error;
    public int data;
}
