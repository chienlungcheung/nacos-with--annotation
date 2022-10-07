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
package com.alibaba.nacos.naming.consistency.ephemeral.distro;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.ServerStatus;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.consistency.ApplyAction;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.ephemeral.EphemeralConsistencyService;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.pojo.Record;
import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 该一致性协议实现名叫 Partition.
 *
 * 该协议实现使用 distro 算法将数据且分为多个块, 每个 Nacos server 节点负责且只负责一块.
 * 数据每一块的生成, 移除和同步都由它对应的 server 来负责. 也就是说, 每个 Nacos server 只负责
 * 处理完整数据的一个子集的写入.
 *
 * 每个 Nacos server 从其它 Nacos servers 同步数据, 所以最后每个 Nacos server 都将
 * 拥有一个完整的数据集.
 * @author nkorange
 * @since 1.0.0
 */
@org.springframework.stereotype.Service("distroConsistencyService")
public class DistroConsistencyServiceImpl implements EphemeralConsistencyService {

    /**
     * 用于同步变更到客户端的后台线程
     */
    private ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);

            t.setDaemon(true);
            t.setName("com.alibaba.nacos.naming.distro.notifier");

            return t;
        }
    });

    /**
     * 用于分配
     */
    @Autowired
    private DistroMapper distroMapper;

    /**
     * 一致性存储抽象
     */
    @Autowired
    private DataStore dataStore;

    /**
     * 用于在数据变更(如 Put)时同步数据给其它 nacos 节点.
     */
    @Autowired
    private TaskDispatcher taskDispatcher;

    @Autowired
    private DataSyncer dataSyncer;

    @Autowired
    private Serializer serializer;

    /**
     * 负责维护 nacos 集群节点更新.
     */
    @Autowired
    private ServerListManager serverListManager;

    @Autowired
    private SwitchDomain switchDomain;

    @Autowired
    private GlobalConfig globalConfig;

    private boolean initialized = false;

    /**
     * 与客户端交互的通知器, 当数据增删时会调用其通知客户端.
     */
    public volatile Notifier notifier = new Notifier();

    private Map<String, CopyOnWriteArrayList<RecordListener>> listeners = new ConcurrentHashMap<>();

    private Map<String, String> syncChecksumTasks = new ConcurrentHashMap<>(16);

    /**
     * 当前节点初始化, 即从其它节点同步数据, 同时启动通知器.
     */
    @PostConstruct
    public void init() {
        GlobalExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    // 从其它节点同步数据
                    load();
                } catch (Exception e) {
                    Loggers.DISTRO.error("load data failed.", e);
                }
            }
        });

        // 启动通知器
        executor.submit(notifier);
    }

    /**
     * 若为集群模式, 等待其它节点上线并同步数据; 否则无须同步数据直接上线.
     */
    public void load() throws Exception {
        // standalone 模式无需同步数据
        if (SystemUtils.STANDALONE_MODE) {
            initialized = true;
            return;
        }
        // 至少两个 server 才能运行集群模式, 如果当前仅一个, 等待另一个上线.
        while (serverListManager.getHealthyServers().size() <= 1) {
            Thread.sleep(1000L);
            Loggers.DISTRO.info("waiting server list init...");
        }

        // 遍历其它节点, 从其同步数据.
        for (Server server : serverListManager.getHealthyServers()) {
            if (NetUtils.localServer().equals(server.getKey())) {
                continue;
            }
            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.debug("sync from " + server);
            }
            // try sync data from remote server:
            if (syncAllDataFromRemote(server)) {
                initialized = true;
                return;
            }
        }
    }

    @Override
    public void put(String key, Record value) throws NacosException {
        onPut(key, value);
        // 数据有更新, 启动一个数据同步任务, 当前 nacos 节点会
        // 将本次变更主动同步给其它 nacos 节点.
        taskDispatcher.addTask(key);
    }

    @Override
    public void remove(String key) throws NacosException {
        onRemove(key);
        listeners.remove(key);
    }

    @Override
    public Datum get(String key) throws NacosException {
        return dataStore.get(key);
    }

    /**
     * 响应 Put 操作
     * @param key
     * @param value
     */
    public void onPut(String key, Record value) {

        if (KeyBuilder.matchEphemeralInstanceListKey(key)) {
            Datum<Instances> datum = new Datum<>();
            datum.value = (Instances) value;
            datum.key = key;
            // 推动时钟前进
            datum.timestamp.incrementAndGet();
            // 放入当前 nacos 节点存储
            dataStore.put(key, datum);
        }

        // 如果没有监听器对该 key 感兴趣则不做通知
        if (!listeners.containsKey(key)) {
            return;
        }

        // 发通知, key 对应数据有变更
        notifier.addTask(key, ApplyAction.CHANGE);
    }

    public void onRemove(String key) {

        dataStore.remove(key);

        if (!listeners.containsKey(key)) {
            return;
        }

        // 发通知, key 对应数据被删除
        notifier.addTask(key, ApplyAction.DELETE);
    }

    /**
     * 负责接收 /distro/checksum API 接口的请求, 比较收到的校验和
     * 是否与本地保存一致, 不一致则进行移除或者更新.
     *
     * @param checksumMap
     * @param server
     */
    public void onReceiveChecksums(Map<String, String> checksumMap, String server) {

        if (syncChecksumTasks.containsKey(server)) {
            // Already in process of this server:
            Loggers.DISTRO.warn("sync checksum task already in process with {}", server);
            return;
        }

        syncChecksumTasks.put(server, "1");

        try {

            List<String> toUpdateKeys = new ArrayList<>();
            List<String> toRemoveKeys = new ArrayList<>();
            for (Map.Entry<String, String> entry : checksumMap.entrySet()) {
                if (distroMapper.responsible(KeyBuilder.getServiceName(entry.getKey()))) {
                    // this key should not be sent from remote server:
                    Loggers.DISTRO.error("receive responsible key timestamp of " + entry.getKey() + " from " + server);
                    // abort the procedure:
                    return;
                }
                // 如果当前 nacos 节点:
                // 1. 不包含校验和中的 key
                // 2. 或者包含的 value 为空
                // 3. 或者包含的 value 的校验和与收到的校验和不同
                // 以上都说明要更新当前 nacos 节点的数据.
                if (!dataStore.contains(entry.getKey()) ||
                    dataStore.get(entry.getKey()).value == null ||
                    !dataStore.get(entry.getKey()).value.getChecksum().equals(entry.getValue())) {
                    toUpdateKeys.add(entry.getKey());
                }
            }

            // 检视当前 nacos 节点中由校验和发送方 server 负责的每个 key
            for (String key : dataStore.keys()) {

                // 检查校验和发送方 server 是否负责 key.
                if (!server.equals(distroMapper.mapSrv(KeyBuilder.getServiceName(key)))) {
                    continue;
                }

                // 如果负责但是不在刚收到的校验和列表里, 则其过期了, 删除之.
                if (!checksumMap.containsKey(key)) {
                    toRemoveKeys.add(key);
                }
            }

            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.info("to remove keys: {}, to update keys: {}, source: {}", toRemoveKeys, toUpdateKeys, server);
            }

            // 移除过期 keys
            for (String key : toRemoveKeys) {
                onRemove(key);
            }

            if (toUpdateKeys.isEmpty()) {
                return;
            }

            try {
                // 从校验和发送方拉取要更新的数据.
                byte[] result = NamingProxy.getData(toUpdateKeys, server);
                processData(result);
            } catch (Exception e) {
                Loggers.DISTRO.error("get data from " + server + " failed!", e);
            }
        } finally {
            // Remove this 'in process' flag:
            syncChecksumTasks.remove(server);
        }

    }

    public boolean syncAllDataFromRemote(Server server) {

        try {
            // 从指定 nacos 节点获取全部 services 以及每个 service 对应的实例列表
            byte[] data = NamingProxy.getAllData(server.getKey());
            processData(data);
            return true;
        } catch (Exception e) {
            Loggers.DISTRO.error("sync full data from " + server + " failed!", e);
            return false;
        }
    }

    public void processData(byte[] data) throws Exception {
        if (data.length > 0) {
            // map 的 key 为 service 名, value 为其对应的 instance 列表
            Map<String, Datum<Instances>> datumMap =
                serializer.deserializeMap(data, Instances.class);

            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {
                dataStore.put(entry.getKey(), entry.getValue());

                if (!listeners.containsKey(entry.getKey())) {
                    // pretty sure the service not exist:
                    if (switchDomain.isDefaultInstanceEphemeral()) {
                        // create empty service
                        Loggers.DISTRO.info("creating service {}", entry.getKey());
                        Service service = new Service();
                        String serviceName = KeyBuilder.getServiceName(entry.getKey());
                        String namespaceId = KeyBuilder.getNamespace(entry.getKey());
                        service.setName(serviceName);
                        service.setNamespaceId(namespaceId);
                        service.setGroupName(Constants.DEFAULT_GROUP);
                        // now validate the service. if failed, exception will be thrown
                        service.setLastModifiedMillis(System.currentTimeMillis());
                        service.recalculateChecksum();
                        listeners.get(KeyBuilder.SERVICE_META_KEY_PREFIX).get(0)
                            .onChange(KeyBuilder.buildServiceMetaKey(namespaceId, serviceName), service);
                    }
                }
            }

            for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {

                if (!listeners.containsKey(entry.getKey())) {
                    // Should not happen:
                    Loggers.DISTRO.warn("listener of {} not found.", entry.getKey());
                    continue;
                }

                try {
                    for (RecordListener listener : listeners.get(entry.getKey())) {
                        listener.onChange(entry.getKey(), entry.getValue().value);
                    }
                } catch (Exception e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] error while execute listener of key: {}", entry.getKey(), e);
                    continue;
                }

                // Update data store if listener executed successfully:
                dataStore.put(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public void listen(String key, RecordListener listener) throws NacosException {
        if (!listeners.containsKey(key)) {
            listeners.put(key, new CopyOnWriteArrayList<>());
        }

        if (listeners.get(key).contains(listener)) {
            return;
        }

        listeners.get(key).add(listener);
    }

    @Override
    public void unlisten(String key, RecordListener listener) throws NacosException {
        if (!listeners.containsKey(key)) {
            return;
        }
        for (RecordListener recordListener : listeners.get(key)) {
            if (recordListener.equals(listener)) {
                listeners.get(key).remove(listener);
                break;
            }
        }
    }

    @Override
    public boolean isAvailable() {
        return isInitialized() || ServerStatus.UP.name().equals(switchDomain.getOverriddenServerStatus());
    }

    public boolean isInitialized() {
        return initialized || !globalConfig.isDataWarmup();
    }

    public class Notifier implements Runnable {

        private ConcurrentHashMap<String, String> services = new ConcurrentHashMap<>(10 * 1024);

        private BlockingQueue<Pair> tasks = new LinkedBlockingQueue<Pair>(1024 * 1024);

        public void addTask(String datumKey, ApplyAction action) {

            // 上一次变更的通知还没发出去, 本次不再追加.
            if (services.containsKey(datumKey) && action == ApplyAction.CHANGE) {
                return;
            }
            if (action == ApplyAction.CHANGE) {
                // 仅将数据变更放入待通知列表, 删除类操作不添加
                services.put(datumKey, StringUtils.EMPTY);
            }
            // 不管是变更还是删除都作为一个通知添加到任务列表
            tasks.add(Pair.with(datumKey, action));
        }

        public int getTaskSize() {
            return tasks.size();
        }

        @Override
        public void run() {
            Loggers.DISTRO.info("distro notifier started");

            while (true) {
                try {

                    Pair pair = tasks.take();

                    if (pair == null) {
                        continue;
                    }

                    String datumKey = (String) pair.getValue0();
                    ApplyAction action = (ApplyAction) pair.getValue1();

                    // 如果存在, 则从待通知列表中将该变更通知移除
                    services.remove(datumKey);

                    int count = 0;

                    if (!listeners.containsKey(datumKey)) {
                        continue;
                    }

                    for (RecordListener listener : listeners.get(datumKey)) {

                        count++;

                        try {
                            if (action == ApplyAction.CHANGE) {
                                // 将 key 数据变更通知给客户端
                                listener.onChange(datumKey, dataStore.get(datumKey).value);
                                continue;
                            }

                            if (action == ApplyAction.DELETE) {
                                // 将 key 数据移除通知给客户端
                                listener.onDelete(datumKey);
                                continue;
                            }
                        } catch (Throwable e) {
                            Loggers.DISTRO.error("[NACOS-DISTRO] error while notifying listener of key: {}", datumKey, e);
                        }
                    }

                    if (Loggers.DISTRO.isDebugEnabled()) {
                        Loggers.DISTRO.debug("[NACOS-DISTRO] datum change notified, key: {}, listener count: {}, action: {}",
                            datumKey, count, action.name());
                    }
                } catch (Throwable e) {
                    Loggers.DISTRO.error("[NACOS-DISTRO] Error while handling notifying task", e);
                }
            }
        }
    }
}
