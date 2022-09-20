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
package com.alibaba.nacos.naming.pojo;

/**
 *
 * 用于 Nacos 集群内传输和存储信息
 *
 * @author nkorange
 * @since 1.0.0
 */
public interface Record {
    /**
     *
     * 获取记录对应的校验和, 该校验和通常用于记录比较
     *
     * @return checksum of record
     */
    String getChecksum();
}
