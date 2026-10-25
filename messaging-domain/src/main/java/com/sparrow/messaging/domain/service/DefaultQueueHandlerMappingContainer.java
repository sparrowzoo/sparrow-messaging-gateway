/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sparrow.messaging.domain.service;


import com.sparrow.messaging.protocol.api.EventHandlerMappingContainer;
import com.sparrow.messaging.protocol.api.SubHandler;
import lombok.NoArgsConstructor;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor
public class DefaultQueueHandlerMappingContainer implements EventHandlerMappingContainer {

    /**
     * {@code map<EventName,MQHandler>}
     */
    private Map<String, SubHandler> queueHandlerMappings = new HashMap<String, com.sparrow.messaging.protocol.api.SubHandler>();

    @Override
    public void put(com.sparrow.messaging.protocol.api.SubHandler handler) {
        String key = null;
        Class parameterClass = (Class) ((ParameterizedType) handler.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        if (parameterClass != null) {
            key = parameterClass.getName();
        }
        if (key != null) {
            queueHandlerMappings.put(key, handler);
        }
    }

    @Override
    public SubHandler get(String eventClazzName) {
        if (this.queueHandlerMappings != null) {
            return queueHandlerMappings.get(eventClazzName);
        }
        return null;
    }
}
