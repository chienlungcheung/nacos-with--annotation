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
package com.alibaba.nacos.naming.core;

import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.servers.ServerChangeListener;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

/**
 * @author nkorange
 */
@Component("distroMapper")
public class DistroMapper implements ServerChangeListener {

    private List<String> healthyList = new ArrayList<>();

    public List<String> getHealthyList() {
        return healthyList;
    }

    @Autowired
    private SwitchDomain switchDomain;

    @Autowired
    private ServerListManager serverListManager;

    /**
     * init server list
     */
    @PostConstruct
    public void init() {
        // 让 serverListManager 有变动时候汇报给自己.
        serverListManager.listen(this);
    }

    public boolean responsible(Cluster cluster, Instance instance) {
        // 不启用服务实例健康检查功能, 都没必要做 distro 分组.
        return switchDomain.isHealthCheckEnabled(cluster.getServiceName())
            && !cluster.getHealthCheckTask().isCancelled()
            && responsible(cluster.getServiceName())
            && cluster.contains(instance);
    }

    /**
     * 排序后, 每个 nacos 节点在当前 nacos 集群中的索引是固定的.
     * 用户的服务名称的哈希值如果对集群取模恰好等于当前 nacos 节点在
     * 集群列表中的索引, 则当前 nacos 节点负责维护这个服务对应的实例存活状态.
     * @param serviceName
     * @return
     */
    public boolean responsible(String serviceName) {
        if (!switchDomain.isDistroEnabled() || SystemUtils.STANDALONE_MODE) {
            return true;
        }

        if (CollectionUtils.isEmpty(healthyList)) {
            // means distro config is not ready yet
            return false;
        }

        // 获取当前 nacos 节点对应的索引
        int index = healthyList.indexOf(NetUtils.localServer());
        // 正常情况 index 和 lastIndex 值一样, 没太看懂为啥这么搞.
        int lastIndex = healthyList.lastIndexOf(NetUtils.localServer());
        if (lastIndex < 0 || index < 0) {
            return true;
        }

        // 计算 serviceName 哈希值以及谁负责维护它
        int target = distroHash(serviceName) % healthyList.size();
        // 如果命中 index 则是当前 nacos 节点负责维护之.
        return target >= index && target <= lastIndex;
    }

    public String mapSrv(String serviceName) {
        if (CollectionUtils.isEmpty(healthyList) || !switchDomain.isDistroEnabled()) {
            return NetUtils.localServer();
        }

        try {
            return healthyList.get(distroHash(serviceName) % healthyList.size());
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("distro mapper failed, return localhost: " + NetUtils.localServer(), e);

            return NetUtils.localServer();
        }
    }

    public int distroHash(String serviceName) {
        return Math.abs(serviceName.hashCode() % Integer.MAX_VALUE);
    }

    /**
     *  Distro 协议只关注 healthy, 忽略这里.
     *
     * @param latestMembers servers after change
     */
    @Override
    public void onChangeServerList(List<Server> latestMembers) {

    }

    /**
     * 用新集群列表替换老的集群列表.
     *
     * @param latestReachableMembers reachable servers after change
     */
    @Override
    public void onChangeHealthyServerList(List<Server> latestReachableMembers) {

        List<String> newHealthyList = new ArrayList<>();
        for (Server server : latestReachableMembers) {
            newHealthyList.add(server.getKey());
        }
        healthyList = newHealthyList;
    }
}
