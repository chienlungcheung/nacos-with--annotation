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
package com.alibaba.nacos.naming.consistency.ephemeral;

import com.alibaba.nacos.naming.consistency.ConsistencyService;

/**
 * 专用于临时(ephemeral)数据的一致性协议.
 *
 * 这个协议服务的数据不要求存在磁盘或数据库里, 因为临时数据会与 server
 * 保持一个 session, 只要 session 还活着, 临时数据就不会丢.
 *
 * 该协议要求写操作要总是成功, 即使发生了网络分区, 也就是说这个协议
 * 是 AP 级别的一致性. 当网络恢复的时候, 数据和每个分区被合并为一个集合,
 * 所以整个集群达成最终一致.
 * @author nkorange
 * @since 1.0.0
 */
public interface EphemeralConsistencyService extends ConsistencyService {
}
