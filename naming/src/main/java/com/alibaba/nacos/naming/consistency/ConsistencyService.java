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
package com.alibaba.nacos.naming.consistency;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.naming.pojo.Record;

/**
 * Consistence service for all implementations to derive.
 * <p>
 * We announce this consistency service to decouple the specific consistency implementation with business logic.
 * User should not be aware of what consistency protocol is being used.
 * <p>
 * In this way, we also provide space for user to extend the underlying consistency protocols, as long as they
 * obey our consistency baseline.
 *
 * 与实现无关的一致性服务接口，将一致性协议实现与业务逻辑解耦，用户也可以自己定义一套一致性协议然后提供这里要求的接口。
 *
 * @author nkorange
 * @since 1.0.0
 */
public interface ConsistencyService {

    /**
     * 将一对 <key, value> 写入 Nacos 集群, key 要用户确保全剧唯一.
     *
     * @param key   key of data, this key should be globally unique
     * @param value value of data
     * @throws NacosException
     * @see
     */
    void put(String key, Record value) throws NacosException;

    /**
     * Remove a data from Nacos cluster
     * <p>
     * 根据 {@code key} 从 Nacos 集群移除相关数据
     *
     * @param key key of data
     * @throws NacosException
     */
    void remove(String key) throws NacosException;

    /**
     * Get a data from Nacos cluster
     * <p>
     * 根据 {@code key} 从 Nacos 集群查询相关数据
     *
     * @param key key of data
     * @return data related to the key
     * @throws NacosException
     */
    Datum get(String key) throws NacosException;

    /**
     * Listen for changes of a data
     * <p>
     * 为与 {@code key} 对应的数据新增一个监听器，以监听 Nacos 集群中相关数据的变化
     *
     * @param key      key of data
     * @param listener callback of data change
     * @throws NacosException
     */
    void listen(String key, RecordListener listener) throws NacosException;

    /**
     * Cancel listening of a data
     * <p>
     * 从与 {@code key} 对应的数据的监听器列表中移除指定的监听器
     *
     * @param key      key of data
     * @param listener callback of data change
     * @throws NacosException
     */
    void unlisten(String key, RecordListener listener) throws NacosException;

    /**
     * Tell the status of this consistency service
     * <p>
     * 检查该一致性服务是否可用
     *
     * @return true if available
     */
    boolean isAvailable();
}
