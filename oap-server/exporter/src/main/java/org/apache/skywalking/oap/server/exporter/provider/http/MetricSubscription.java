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

import com.google.gson.Gson;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MetricSubscription {
    private String cmpAddr = "";
    private final int pageSize = 1024;
    private long version = -1;
    private OkHttpClient httpClient;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    HashMap<String, Set<String>> subscriptions = new HashMap<>();

    private final int interval = 120;
    private ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricSubscription.class);

    public boolean contains(String metric, String id) {
        boolean ret = false;
        lock.readLock().lock();
        Set<String> set = subscriptions.get(metric);
        if (set != null) {
            ret = set.contains(id);
        }
        lock.readLock().unlock();
        return ret;
    }

    public MetricSubscription(String addr) {
        this.cmpAddr = addr;
    }

    public boolean update() {
        MetricSubResp resp = getFromAutoCmp(cmpAddr, 1);
        if (resp == null) {
            return false;
        }

        if (resp.version == this.version) {
            return true;
        }

        //all
        if (resp.isAll) {
            HashMap<String, Set<String>> newMap = new HashMap<>();
            newMap = addTo(resp, newMap);
            boolean isOk = true;
            int i = 2;
            while (true) {
                MetricSubResp temp = getFromAutoCmp(cmpAddr, i);
                if (temp == null) {
                    isOk = false;
                    break;
                }
                if (temp.count == 0 || temp.details == null) {
                    break;
                }
                i += 1;
                newMap = addTo(resp, newMap);
            }
            if (isOk) {
                this.version = resp.version;
                lock.writeLock().lock();
                this.subscriptions = newMap;
                lock.writeLock().unlock();

                LOGGER.info("MetricSubscription update  version {}", this.version);
                return true;
            } else {
                LOGGER.info("MetricSubscription can't update  version {}", this.version);
                return false;
            }
        }

        //part
        if (resp.details == null) {
            return true;
        }

        for (int i = 0; i < resp.details.length; i++) {
            MetricSubRespDetail detail = resp.details[i];
            if (detail.items == null || detail.items.length == 0) {
                continue;
            }
            switch (detail.type) {
                case "ADD":
                    addOp(detail.items);
                    break;
                case "DEL":
                    delOp(detail.items);
                    break;
                default:
            }
        }
        this.version = resp.version;
        LOGGER.info("MetricSubscription update part version {}", this.version);
        return true;
    }

    private MetricSubResp getFromAutoCmp(String addr, int page) {
        MetricSubResp ret = null;
        String body = null;
        Response response = null;
        try {
            String url = String.format("%s/apm/syncMetric?page=%d&size=%d&oldVersion=%d", addr, page, pageSize, version);
            Request request = new Request.Builder().url(url).get().build();
            response = httpClient.newCall(request).execute();
            if (response.code() != 200) {
                LOGGER.error("getFromAutoCmp response.code: {}", response.code());
                return null;
            }

            Gson gson = new Gson();
            body = response.body().string();
            AutoCmpMetricSubResp cmpResp = gson.fromJson(body, AutoCmpMetricSubResp.class);
            if (cmpResp != null) {
                if (cmpResp.code == 1) {
                    ret = cmpResp.data;
                } else {
                    LOGGER.error("getFromAutoCmp error: {}", cmpResp.error);
                }
            }
        } catch (Exception e) {
            if (body != null) {
                LOGGER.error("getFromAutoCmp error: {}", body);
            } else {
                LOGGER.error("getFromAutoCmp error: {}", e.getMessage());
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }
        return ret;
    }

    private void addOp(String[][] items) {
        Lock wLock = lock.writeLock();
        wLock.lock();
        for (int i = 0; i < items.length; i++) {
            String[] item = items[i];
            if (item.length != 2) {
                continue;
            }
            Set<String> set = subscriptions.get(item[0]);
            if (set == null) {
                set = new HashSet<>();
                subscriptions.put(item[0], set);
            }
            set.add(item[1]);
        }
        wLock.unlock();
    }

    private void delOp(String[][] items) {
        Lock wLock = lock.writeLock();
        wLock.lock();
        for (int i = 0; i < items.length; i++) {
            String[] item = items[i];
            if (item.length != 2) {
                continue;
            }
            Set<String> set = subscriptions.get(item[0]);
            if (set != null) {
                set.remove(item[1]);
            }
        }
        wLock.unlock();
    }

    public boolean runSubscription() {
        httpClient = new OkHttpClient.Builder().writeTimeout(1, TimeUnit.MINUTES).readTimeout(1, TimeUnit.MINUTES).build();
        update();

        TimerTask timerTask = new TimerTask(this);
        timer.scheduleAtFixedRate(timerTask, interval, interval, TimeUnit.SECONDS);
        return true;
    }

    private HashMap<String, Set<String>> addTo(MetricSubResp resp, HashMap<String, Set<String>> map) {
        if (resp.details == null || resp.details.length != 1) {
            return map;
        }

        String[][] data = resp.details[0].items;
        for (int i = 0; i < data.length; i++) {
            String[] items = data[i];
            if (items.length == 2) {
                Set<String> s = map.get(items[0]);
                if (s == null) {
                    s = new HashSet<>();
                    map.put(items[0], s);
                }
                s.add(items[1]);
            }
        }
        return map;
    }
}

class TimerTask implements Runnable {
    MetricSubscription subscription = null;

    public TimerTask(MetricSubscription ms) {
        subscription = ms;
    }

    // @Override
    public void run() {
        subscription.update();
    }
}

class MetricSubResp {
    public int count;
    public long version;
    public boolean isAll;
    public MetricSubRespDetail[] details;
}

class MetricSubRespDetail {
    String type;
    String[][] items;
}

class AutoCmpMetricSubResp {
    public int code;
    public String error;
    public MetricSubResp data;
}