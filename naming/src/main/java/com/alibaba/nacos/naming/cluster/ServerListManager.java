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
package com.alibaba.nacos.naming.cluster;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.servers.ServerChangeListener;
import com.alibaba.nacos.naming.misc.*;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.nacos.core.utils.SystemUtils.*;

/**
 * The manager to globally refresh and operate server list.
 *
 * @author nkorange
 * @since 1.0.0
 */
@Component("serverListManager")
public class ServerListManager {

    private static final int STABLE_PERIOD = 60 * 1000;

    @Autowired
    private SwitchDomain switchDomain;

    private List<ServerChangeListener> listeners = new ArrayList<>();

    private List<Server> servers = new ArrayList<>();

    private List<Server> healthyServers = new ArrayList<>();

    private Map<String, List<Server>> distroConfig = new ConcurrentHashMap<>();

    private Map<String, Long> distroBeats = new ConcurrentHashMap<>(16);

    private Set<String> liveSites = new HashSet<>();

    private final static String LOCALHOST_SITE = UtilsAndCommons.UNKNOWN_SITE;

    private long lastHealthServerMillis = 0L;

    private boolean autoDisabledHealthCheck = false;

    private Synchronizer synchronizer = new ServerStatusSynchronizer();

    public void listen(ServerChangeListener listener) {
        listeners.add(listener);
    }

    @PostConstruct
    public void init() {
        GlobalExecutor.registerServerListUpdater(new ServerListUpdater());
        GlobalExecutor.registerServerStatusReporter(new ServerStatusReporter(), 5000);
    }

    /**
     * 从配置文件或环境变量获取 nacos 集群列表.
     * @return
     */
    private List<Server> refreshServerList() {

        List<Server> result = new ArrayList<>();

        if (STANDALONE_MODE) {
            Server server = new Server();
            server.setIp(NetUtils.getLocalAddress());
            server.setServePort(RunningConfig.getServerPort());
            result.add(server);
            return result;
        }

        List<String> serverList = new ArrayList<>();
        try {
            // 读取配置文件 cluster.conf 获取最新的 nacos 集群列表
            serverList = readClusterConf();
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("failed to get config: " + CLUSTER_CONF_FILE_PATH, e);
        }

        if (Loggers.SRV_LOG.isDebugEnabled()) {
            Loggers.SRV_LOG.debug("SERVER-LIST from cluster.conf: {}", result);
        }

        // 如果配置文件不存在, 也支持从环境变量 SELF_SERVICE_CLUSTER_ENV 获取 nacos 集群列表
        if (CollectionUtils.isEmpty(serverList)) {
            serverList = SystemUtils.getIPsBySystemEnv(UtilsAndCommons.SELF_SERVICE_CLUSTER_ENV);
            if (Loggers.SRV_LOG.isDebugEnabled()) {
                Loggers.SRV_LOG.debug("SERVER-LIST from system variable: {}", result);
            }
        }

        if (CollectionUtils.isNotEmpty(serverList)) {

            for (int i = 0; i < serverList.size(); i++) {

                String ip;
                int port;
                String server = serverList.get(i);
                if (server.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
                    ip = server.split(UtilsAndCommons.IP_PORT_SPLITER)[0];
                    port = Integer.parseInt(server.split(UtilsAndCommons.IP_PORT_SPLITER)[1]);
                } else {
                    ip = server;
                    port = RunningConfig.getServerPort();
                }

                Server member = new Server();
                member.setIp(ip);
                member.setServePort(port);
                result.add(member);
            }
        }

        return result;
    }

    public boolean contains(String s) {
        for (Server server : servers) {
            if (server.getKey().equals(s)) {
                return true;
            }
        }
        return false;
    }

    public List<Server> getServers() {
        return servers;
    }

    public List<Server> getHealthyServers() {
        return healthyServers;
    }

    private void notifyListeners() {

        GlobalExecutor.notifyServerListChange(new Runnable() {
            @Override
            public void run() {
                for (ServerChangeListener listener : listeners) {
                    listener.onChangeServerList(servers);
                    listener.onChangeHealthyServerList(healthyServers);
                }
            }
        });
    }

    public Map<String, List<Server>> getDistroConfig() {
        return distroConfig;
    }

    /**
     * 处理通过 /operator/server/status 接口收到的状态报告(site:ip:lastReportTime:weight).
     * @param configInfo
     */
    public synchronized void onReceiveServerStatus(String configInfo) {

        Loggers.SRV_LOG.info("receive config info: {}", configInfo);

        String[] configs = configInfo.split("\r\n");
        if (configs.length == 0) {
            return;
        }

        List<Server> newHealthyList = new ArrayList<>();
        List<Server> tmpServerList = new ArrayList<>();

        for (String config : configs) {
            tmpServerList.clear();
            // site:ip:lastReportTime:weight
            String[] params = config.split("#");
            if (params.length <= 3) {
                Loggers.SRV_LOG.warn("received malformed distro map data: {}", config);
                continue;
            }

            Server server = new Server();

            server.setSite(params[0]);
            server.setIp(params[1].split(UtilsAndCommons.IP_PORT_SPLITER)[0]);
            server.setServePort(Integer.parseInt(params[1].split(UtilsAndCommons.IP_PORT_SPLITER)[1]));
            server.setLastRefTime(Long.parseLong(params[2]));

            if (!contains(server.getKey())) {
                throw new IllegalArgumentException("server: " + server.getKey() + " is not in serverlist");
            }

            Long lastBeat = distroBeats.get(server.getKey());
            long now = System.currentTimeMillis();
            if (null != lastBeat) {
                // 检查发送报告的 nacos 节点上次发送心跳距今间隔是否超过限制.
                // 这里有个特殊情况, 即使收到了状态报告但是因为间隔超过限制,
                // 这个节点也会被设置为非活跃, 但当前收到报告明明说明它还活着, 看着矛盾.
                // 不过不要紧, 这个应该是为了避免该节点不稳定, 先不着急把它归为活跃, 待
                // 下次按时上报状态, 它会被重新置为活跃, 连续两次报告上报正常, 才算重归正常.
                server.setAlive(now - lastBeat < switchDomain.getDistroServerExpiredMillis());
            }
            // 用当前时间戳更新发送报告的 nacos 节点对应的心跳时间戳.
            distroBeats.put(server.getKey(), now);

            Date date = new Date(Long.parseLong(params[2]));
            // 记录报告刷新时间.
            server.setLastRefTimeStr(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date));

            server.setWeight(params.length == 4 ? Integer.parseInt(params[3]) : 1);
            List<Server> list = distroConfig.get(server.getSite());
            if (list == null || list.size() <= 0) {
                list = new ArrayList<>();
                list.add(server);
                distroConfig.put(server.getSite(), list);
            }

            for (Server s : list) {
                String serverId = s.getKey() + "_" + s.getSite();
                String newServerId = server.getKey() + "_" + server.getSite();

                if (serverId.equals(newServerId)) {
                    if (s.isAlive() != server.isAlive() || s.getWeight() != server.getWeight()) {
                        Loggers.SRV_LOG.warn("server beat out of date, current: {}, last: {}",
                            JSON.toJSONString(server), JSON.toJSONString(s));
                    }
                    tmpServerList.add(server);
                    continue;
                }
                tmpServerList.add(s);
            }

            if (!tmpServerList.contains(server)) {
                tmpServerList.add(server);
            }

            distroConfig.put(server.getSite(), tmpServerList);
        }
        liveSites.addAll(distroConfig.keySet());
    }

    public void clean() {
        cleanInvalidServers();

        for (Map.Entry<String, List<Server>> entry : distroConfig.entrySet()) {
            for (Server server : entry.getValue()) {
                //request other server to clean invalid servers
                if (!server.getKey().equals(NetUtils.localServer())) {
                    requestOtherServerCleanInvalidServers(server.getKey());
                }
            }

        }
    }

    public Set<String> getLiveSites() {
        return liveSites;
    }

    private void cleanInvalidServers() {
        for (Map.Entry<String, List<Server>> entry : distroConfig.entrySet()) {
            List<Server> currentServers = entry.getValue();
            if (null == currentServers) {
                distroConfig.remove(entry.getKey());
                continue;
            }

            currentServers.removeIf(server -> !server.isAlive());
        }
    }

    private void requestOtherServerCleanInvalidServers(String serverIP) {
        Map<String, String> params = new HashMap<String, String>(1);

        params.put("action", "without-diamond-clean");
        try {
            NamingProxy.reqAPI("distroStatus", params, serverIP, false);
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("[DISTRO-STATUS-CLEAN] Failed to request to clean server status to " + serverIP, e);
        }
    }

    /**
     * 该任务周期性运行, 负责检测 nacos 集群变化.
     * 如果有节点上下线, 会及时将最新集群列表通知到 distro 和 raft 集群.
     */
    public class ServerListUpdater implements Runnable {

        @Override
        public void run() {
            try {
                // 获取最新的 nacos 集群列表.
                List<Server> refreshedServers = refreshServerList();
                List<Server> oldServers = servers;

                if (CollectionUtils.isEmpty(refreshedServers)) {
                    Loggers.RAFT.warn("refresh server list failed, ignore it.");
                    return;
                }

                boolean changed = false;

                // 新集群与老集群差集为新增的 nacos 节点, 吸收之.
                List<Server> newServers = (List<Server>) CollectionUtils.subtract(refreshedServers, oldServers);
                if (CollectionUtils.isNotEmpty(newServers)) {
                    servers.addAll(newServers);
                    changed = true;
                    Loggers.RAFT.info("server list is updated, new: {} servers: {}", newServers.size(), newServers);
                }

                // 老集群与新集群差集为被移除的 nacos 节点, 移除之.
                List<Server> deadServers = (List<Server>) CollectionUtils.subtract(oldServers, refreshedServers);
                if (CollectionUtils.isNotEmpty(deadServers)) {
                    servers.removeAll(deadServers);
                    changed = true;
                    Loggers.RAFT.info("server list is updated, dead: {}, servers: {}", deadServers.size(), deadServers);
                }

                // nacos 集群节点有变动, 通知监听器,
                // Distro 协议和 Raft 协议各自实现了监听器,
                // 它们各自都会关注集群列表的变化.
                if (changed) {
                    notifyListeners();
                }

            } catch (Exception e) {
                Loggers.RAFT.info("error while updating server list.", e);
            }
        }
    }


    /**
     * 周期性任务, 专用于 distro 协议(raft 本身会互相发送心跳检测存活).
     * 负责生成当前 nacos 节点状态报告并发送给
     * 每个 nacos 节点(也发给自己).
     */
    private class ServerStatusReporter implements Runnable {

        @Override
        public void run() {
            try {

                if (RunningConfig.getServerPort() <= 0) {
                    return;
                }

                // 通过心跳间隔是否超过限制来判断 nacos 集群活跃节点是否有变化.
                checkDistroHeartbeat();

                int weight = Runtime.getRuntime().availableProcessors() / 2;
                if (weight <= 0) {
                    weight = 1;
                }

                long curTime = System.currentTimeMillis();
                // 生成当前 nacos 节点的状态报告, 很简单就是一个四元组.
                String status = LOCALHOST_SITE + "#" + NetUtils.localServer() + "#" + curTime + "#" + weight + "\r\n";

                // 发送自己的状态报告给自己.
                onReceiveServerStatus(status);

                // 当前 nacos 集群列表(注意这个是配置文件或者环境变量设置的, 不一定都活着).
                List<Server> allServers = getServers();

                if (!contains(NetUtils.localServer())) {
                    Loggers.SRV_LOG.error("local ip is not in serverlist, ip: {}, serverlist: {}", NetUtils.localServer(), allServers);
                    return;
                }

                // 将当前 nacos 节点状态报告发送个其它 nacos 节点.
                if (allServers.size() > 0 && !NetUtils.localServer().contains(UtilsAndCommons.LOCAL_HOST_IP)) {
                    for (com.alibaba.nacos.naming.cluster.servers.Server server : allServers) {
                        if (server.getKey().equals(NetUtils.localServer())) {
                            continue;
                        }

                        Message msg = new Message();
                        msg.setData(status);

                        // 发送给 /v1/ns/operator/server/status 接口.
                        synchronizer.send(server.getKey(), msg);

                    }
                }
            } catch (Exception e) {
                Loggers.SRV_LOG.error("[SERVER-STATUS] Exception while sending server status", e);
            } finally {
                GlobalExecutor.registerServerStatusReporter(this, switchDomain.getServerStatusSynchronizationPeriodMillis());
            }

        }
    }

    /**
     * 该方法专用于 Distro 协议, 用于检测遵循该协议的 nacos 节点互相
     * 检测对方是否活着, 依据为状态报告上报时间间隔是否超限.
     *
     * Raft 协议本身各节点互相发送心跳, 所以无需这个方法在检查存活.
     */
    private void checkDistroHeartbeat() {

        Loggers.SRV_LOG.debug("check distro heartbeat.");

        List<Server> servers = distroConfig.get(LOCALHOST_SITE);
        if (CollectionUtils.isEmpty(servers)) {
            return;
        }

        List<Server> newHealthyList = new ArrayList<>(servers.size());
        long now = System.currentTimeMillis();
        for (Server s: servers) {
            Long lastBeat = distroBeats.get(s.getKey());
            if (null == lastBeat) {
                continue;
            }
            // 如果上次心跳距今间隔超过了限制则挂, 否则就是活跃.
            s.setAlive(now - lastBeat < switchDomain.getDistroServerExpiredMillis());
        }

        //local site servers
        List<String> allLocalSiteSrvs = new ArrayList<>();
        for (Server server : servers) {

            if (server.getKey().endsWith(":0")) {
                continue;
            }

            server.setAdWeight(switchDomain.getAdWeight(server.getKey()) == null ? 0 : switchDomain.getAdWeight(server.getKey()));

            // TODO 循环内 if 各自执行一次, 而且 server.equals 只检查 ip 和 port, 那这个循环的意义何在?
            for (int i = 0; i < server.getWeight() + server.getAdWeight(); i++) {

                if (!allLocalSiteSrvs.contains(server.getKey())) {
                    allLocalSiteSrvs.add(server.getKey());
                }

                // 与上面不同, 这里添加 server 的时候除了检查是否在列表里外, 还要看看是否活跃.
                if (server.isAlive() && !newHealthyList.contains(server)) {
                    newHealthyList.add(server);
                }
            }
        }

        Collections.sort(newHealthyList);
        // 如果活跃的 nacos 节点少于全量, 则 ratio 就是一个小于 1 的数值.
        float curRatio = (float) newHealthyList.size() / allLocalSiteSrvs.size();

        if (autoDisabledHealthCheck
            && curRatio > switchDomain.getDistroThreshold()
            && System.currentTimeMillis() - lastHealthServerMillis > STABLE_PERIOD) {
            Loggers.SRV_LOG.info("[NACOS-DISTRO] distro threshold restored and " +
                "stable now, enable health check. current ratio: {}", curRatio);

            switchDomain.setHealthCheckEnabled(true);

            // we must set this variable, otherwise it will conflict with user's action
            autoDisabledHealthCheck = false;
        }

        // 如果本轮心跳检查发现 nacos 集群活跃节点有变化, 则通知 Distro 协议更新集群健康节点列表.
        if (!CollectionUtils.isEqualCollection(healthyServers, newHealthyList)) {
            // for every change disable healthy check for some while
            if (switchDomain.isHealthCheckEnabled()) {
                Loggers.SRV_LOG.info("[NACOS-DISTRO] healthy server list changed, " +
                        "disable health check for {} ms from now on, old: {}, new: {}", STABLE_PERIOD,
                    healthyServers, newHealthyList);

                switchDomain.setHealthCheckEnabled(false);
                autoDisabledHealthCheck = true;

                lastHealthServerMillis = System.currentTimeMillis();
            }

            healthyServers = newHealthyList;
            notifyListeners();
        }
    }

}
