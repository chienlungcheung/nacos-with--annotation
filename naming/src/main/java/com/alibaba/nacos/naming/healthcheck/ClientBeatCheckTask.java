/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.healthcheck;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.boot.SpringContext;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.healthcheck.events.InstanceHeartbeatTimeoutEvent;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.push.PushService;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;

import java.net.HttpURLConnection;
import java.util.List;


/**
 * 基于每个服务实例上次心跳时间戳, 判断实例是否超时或者挂了,
 * 不管是超时还是挂了都会发送消息给订阅发现该服务的客户端, 如果
 * 挂了则会将实例从列表删除并在 nacos 集群之间同步最新的实例列表.
 * @author nkorange
 */
public class ClientBeatCheckTask implements Runnable {

    private Service service;

    public ClientBeatCheckTask(Service service) {
        this.service = service;
    }


    @JSONField(serialize = false)
    public PushService getPushService() {
        return SpringContext.getAppContext().getBean(PushService.class);
    }

    @JSONField(serialize = false)
    public DistroMapper getDistroMapper() {
        return SpringContext.getAppContext().getBean(DistroMapper.class);
    }

    public GlobalConfig getGlobalConfig() {
        return SpringContext.getAppContext().getBean(GlobalConfig.class);
    }

    public String taskKey() {
        return service.getName();
    }

    @Override
    public void run() {
        try {
            // 只针对当前 nacos 节点负责的服务的实例进行心跳检测.
            if (!getDistroMapper().responsible(service.getName())) {
                return;
            }

            List<Instance> instances = service.allIPs(true);

            // first set health status of instances:
            for (Instance instance : instances) {
                // 如果服务实力上次发送心跳至今已经超过了超时限制(15 秒), 则认为该服务实例不活跃了.
                // 这次检测还不至于将其从列表驱逐, 下面的那个超时才会.
                if (System.currentTimeMillis() - instance.getLastBeat() > instance.getInstanceHeartBeatTimeOut()) {
                    if (!instance.isMarked()) {
                        if (instance.isHealthy()) {
                            instance.setHealthy(false);
                            Loggers.EVT_LOG.info("{POS} {IP-DISABLED} valid: {}:{}@{}@{}, region: {}, msg: client timeout after {}, last beat: {}",
                                instance.getIp(), instance.getPort(), instance.getClusterName(), service.getName(),
                                UtilsAndCommons.LOCALHOST_SITE, instance.getInstanceHeartBeatTimeOut(), instance.getLastBeat());
                            // 通知服务发现的订阅者, 有服务实例出问题了.
                            // 这个消息会通过 PushService push 给订阅者.
                            getPushService().serviceChanged(service);
                            SpringContext.getAppContext().publishEvent(new InstanceHeartbeatTimeoutEvent(this, instance));
                        }
                    }
                }
            }

            if (!getGlobalConfig().isExpireInstance()) {
                return;
            }

            // 如果有服务实例上次发送心跳距今已经超过超时限制(30 秒)未发送心跳,
            // 则认为其挂掉了, 从列表删除.
            for (Instance instance : instances) {

                if (instance.isMarked()) {
                    continue;
                }

                if (System.currentTimeMillis() - instance.getLastBeat() > instance.getIpDeleteTimeout()) {
                    // delete instance
                    Loggers.SRV_LOG.info("[AUTO-DELETE-IP] service: {}, ip: {}", service.getName(), JSON.toJSONString(instance));
                    deleteIP(instance);
                }
            }

        } catch (Exception e) {
            Loggers.SRV_LOG.warn("Exception while processing client beat time out.", e);
        }

    }


    private void deleteIP(Instance instance) {

        try {
            NamingProxy.Request request = NamingProxy.Request.newRequest();
            request.appendParam("ip", instance.getIp())
                .appendParam("port", String.valueOf(instance.getPort()))
                .appendParam("ephemeral", "true")
                .appendParam("clusterName", instance.getClusterName())
                .appendParam("serviceName", service.getName())
                .appendParam("namespaceId", service.getNamespaceId());

            String url = "http://127.0.0.1:" + RunningConfig.getServerPort() + RunningConfig.getContextPath()
                + UtilsAndCommons.NACOS_NAMING_CONTEXT + "/instance?" + request.toUrl();

            // delete instance asynchronously:
            HttpClient.asyncHttpDelete(url, null, null, new AsyncCompletionHandler() {
                @Override
                public Object onCompleted(Response response) throws Exception {
                    if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                        Loggers.SRV_LOG.error("[IP-DEAD] failed to delete ip automatically, ip: {}, caused {}, resp code: {}",
                            instance.toJSON(), response.getResponseBody(), response.getStatusCode());
                    }
                    return null;
                }
            });

        } catch (Exception e) {
            Loggers.SRV_LOG.error("[IP-DEAD] failed to delete ip automatically, ip: {}, error: {}", instance.toJSON(), e);
        }
    }
}
