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
import org.apache.skywalking.oap.server.core.analysis.IDManager;
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
import org.apache.skywalking.oap.server.core.source.DefaultScopeDefine;
import org.apache.skywalking.oap.server.exporter.provider.MetricFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpExporter extends MetricFormatter implements MetricValuesExportService, IConsumer<ExportData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpExporter.class);
    private HttpExporterSetting setting;
    private final DataCarrier exportBuffer;

    private final OkHttpClient httpClient;
    private static final MediaType jsonMediaType = MediaType.parse("application/json; charset=utf-8");

    private MetricSubscription subscription;
    private HashMap<String, CmpMetric> metricCache = new HashMap<String, CmpMetric>();


    public HttpExporter(HttpExporterSetting setting) {
        this.setting = setting;
        exportBuffer = new DataCarrier<ExportData>(setting.getBufferChannelNum(), setting.getBufferChannelSize());
        exportBuffer.consume(this, 1, 1000);
        httpClient = new OkHttpClient.Builder().writeTimeout(1, TimeUnit.MINUTES).readTimeout(1, TimeUnit.MINUTES).build();

        String cmpAddr = setting.getCmpAddr();
        if (cmpAddr.length() == 0) {
            if (setting.getCmpGateWay().contains("monitor-insight-gateway-test")) {
                cmpAddr = "http://auto-cloud-monitor-lf-pre.openapi.corpautohome.com/api/v1";
            } else {
                cmpAddr = "http://auto-cloud-monitor.openapi.corpautohome.com/api/v1";
            }
        }

        subscription = new MetricSubscription(cmpAddr);
    }

    @Override
    public void export(ExportEvent event) {
        if (ExportEvent.EventType.TOTAL == event.getType()) {
            Metrics metrics = event.getMetrics();
            if (metrics instanceof WithMetadata) {
                MetricsMetaInfo meta = ((WithMetadata) metrics).getMeta();
                if (subscription.contains(meta.getMetricsName(), meta.getId())) {
                    exportBuffer.produce(new ExportData(meta, metrics));
                }
            }
        }
    }

    public void initSubscriptionList() {
        // LOGGER.debug("Get exporter subscription list, {}", subscriptionSet);
        subscription.runSubscription();
    }

    @Override
    public void init() {

    }

    @Override
    public void consume(List<ExportData> data) {
        if (data.size() > 0) {
            send(data);

            if (metricCache.size() > 200000) {
                metricCache.clear();
            }
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
        final int batchCount = 1024;
        int cap = batchCount;
        if (data.size() < cap) {
            cap = data.size();
        }

        int j = 0;
        ExportData temp = null;
        List<CmpMetric> ms = new ArrayList<CmpMetric>(cap + 1);

        //
        Calendar now = Calendar.getInstance();
        int minute = now.get(Calendar.MINUTE);
        for (int i = 0; i < data.size(); i++) {
            temp = data.get(i);
            if (temp.getMetrics().getTimeBucket() % 100 == minute) {
                continue;
            }

            CmpMetric m = toCmpMetric(temp);
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

                    String respBody = response.body().string();
                    if (respBody.length() > 256) {
                        LOGGER.error("Post Metrics error:  ret {}", respBody);
                        LOGGER.error("Post Metrics error:  data {}", jsonStr);
                        return;
                    }

                    MetricPostResp ret = gson.fromJson(respBody, MetricPostResp.class);
                    if (ret != null && (ret.code == 0)) {
                        LOGGER.error("Post Metrics error:  ret {}", respBody);
                    }
                    LOGGER.info("Post Metrics size: {}, time:{}ms", data.size(), System.currentTimeMillis() - begin);
                }
            });
        } catch (Exception e) {
            LOGGER.error("Post Metrics Exception: " + e.getMessage());
        }
    }

    private CmpMetric toCmpMetric(ExportData d) {
        CmpMetric cm = getCmpMetric(d.getMeta());
        Metrics metrics = d.getMetrics();
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

        //LOGGER.info(cm.metric + " " + d.getMeta().toString() + " " + metrics.getTimeBucket() + "===" + cm.value);
        return cm;
    }

    private CmpMetric getCmpMetric(MetricsMetaInfo meta) {
        final String metaId = meta.getId();
        final String metricName = meta.getMetricsName();
        final String key = metricName + metaId;
        CmpMetric cm = metricCache.get(key);
        if (cm != null) {
            return cm;
        }

        cm = new CmpMetric();
        cm.metric = metricName;
        cm.tags = new String[2][];

        String name = "name";
        String service = "service";
        int scope = meta.getScope();
        if (DefaultScopeDefine.inServiceCatalog(scope)) {
            final IDManager.ServiceID.ServiceIDDefinition serviceIDDefinition = IDManager.ServiceID.analysisId(metaId);
            name = serviceIDDefinition.getName();
            service = name;
        } else if (DefaultScopeDefine.inServiceInstanceCatalog(scope)) {
            final IDManager.ServiceInstanceID.InstanceIDDefinition instanceIDDefinition = IDManager.ServiceInstanceID.analysisId(metaId);
            name = instanceIDDefinition.getName();
            IDManager.ServiceID.ServiceIDDefinition serviceIDDefinition = IDManager.ServiceID.analysisId(instanceIDDefinition.getServiceId());
            service = serviceIDDefinition.getName();
        } else if (DefaultScopeDefine.inEndpointCatalog(scope)) {
            final IDManager.EndpointID.EndpointIDDefinition endpointIDDefinition = IDManager.EndpointID.analysisId(metaId);
            name = endpointIDDefinition.getEndpointName();
            IDManager.ServiceID.ServiceIDDefinition serviceIDDefinition = IDManager.ServiceID.analysisId(
                    endpointIDDefinition.getServiceId());
            service = serviceIDDefinition.getName();
        } else {
        }

        if (name == null) {
            name = "name";
        }
        if (service == null) {
            service = "service";
        }

        cm.tags[0] = new String[]{"name", name};
        cm.tags[1] = new String[]{"service", service};

        metricCache.put(key, cm);
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

