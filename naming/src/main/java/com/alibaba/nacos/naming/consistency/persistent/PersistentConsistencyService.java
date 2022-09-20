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
package com.alibaba.nacos.naming.consistency.persistent;

import com.alibaba.nacos.naming.consistency.ConsistencyService;

/**
 * 实现该接口的都能保证 CP 级别一致性, 这意味着:
 * 一旦写操作的响应为成功, 相关数据就被保证成功写入了集群, 而且,
 * 协议保证数据在各个 server 之间是一致的.
 * @author nkorange
 * @since 1.0.0
 */
public interface PersistentConsistencyService extends ConsistencyService {
}
