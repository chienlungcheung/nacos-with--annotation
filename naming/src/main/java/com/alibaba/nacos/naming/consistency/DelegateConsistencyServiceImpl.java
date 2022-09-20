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
import com.alibaba.nacos.naming.consistency.ephemeral.EphemeralConsistencyService;
import com.alibaba.nacos.naming.consistency.persistent.PersistentConsistencyService;
import com.alibaba.nacos.naming.pojo.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 代理类, 实现了 ConsistencyService 接口, 但实际上是
 * 对其它一致性协议实现的封装. 使用时候根据 key 类型进行
 * 具体的一致性协议实现的对应.
 *
 * @author nkorange
 * @since 1.0.0
 */
@Service("consistencyDelegate")
public class DelegateConsistencyServiceImpl implements ConsistencyService {

    @Autowired
    private PersistentConsistencyService persistentConsistencyService;

    @Autowired
    private EphemeralConsistencyService ephemeralConsistencyService;

    /**
     * 写入数据
     * @param key   key of data, this key should be globally unique
     * @param value value of data
     * @throws NacosException
     */
    @Override
    public void put(String key, Record value) throws NacosException {
        mapConsistencyService(key).put(key, value);
    }

    /**
     * 移除数据
     * @param key key of data
     * @throws NacosException
     */
    @Override
    public void remove(String key) throws NacosException {
        mapConsistencyService(key).remove(key);
    }

    /**
     * 读取指定数据
     * @param key key of data
     * @return
     * @throws NacosException
     */
    @Override
    public Datum get(String key) throws NacosException {
        return mapConsistencyService(key).get(key);
    }

    /**
     * 将某个数据加入变更监听列表
     * @param key      key of data
     * @param listener callback of data change
     * @throws NacosException
     */
    @Override
    public void listen(String key, RecordListener listener) throws NacosException {

        // 特殊 key, 需要被两个级别一致性协议同时监听
        if (KeyBuilder.SERVICE_META_KEY_PREFIX.equals(key)) {
            persistentConsistencyService.listen(key, listener);
            ephemeralConsistencyService.listen(key, listener);
            return;
        }

        mapConsistencyService(key).listen(key, listener);
    }

    /**
     * 将指定数据从监听列表移除
     * @param key      key of data
     * @param listener callback of data change
     * @throws NacosException
     */
    @Override
    public void unlisten(String key, RecordListener listener) throws NacosException {
        mapConsistencyService(key).unlisten(key, listener);
    }

    /**
     * 当前一致性实现是否可用
     * @return
     */
    @Override
    public boolean isAvailable() {
        return ephemeralConsistencyService.isAvailable() && persistentConsistencyService.isAvailable();
    }

    /**
     * 确定某个数据具体用哪个一致性实现
     * @param key
     * @return
     */
    private ConsistencyService mapConsistencyService(String key) {
        return KeyBuilder.matchEphemeralKey(key) ? ephemeralConsistencyService : persistentConsistencyService;
    }
}
