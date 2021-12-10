/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cqz.flink.connectors.redis.common.handler;

import com.cqz.flink.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.configuration.ReadableConfig;

/**
 * handler to create flink jedis config.
 */
public interface FlinkJedisConfigHandler extends RedisHandler  {

    /**
     * create flink jedis config use sepecified properties.
     * @param config used to create flink jedis config
     * @return flink jedis config
     */
    FlinkJedisConfigBase createFlinkJedisConfig(ReadableConfig config);
}
