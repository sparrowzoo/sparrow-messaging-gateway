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

package com.sparrow.messaging.domain.service.pub.sub.mapping;


import com.sparrow.messaging.protocol.api.EventHandlerMappingContainer;
import com.sparrow.messaging.protocol.api.Subscriber;
import com.sparrow.messaging.protocol.Event;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.reflect.TypeUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@NoArgsConstructor
public class PubSubMappingContainer implements EventHandlerMappingContainer {

    /**
     * {@code map<String,Subscriber>}
     */
    private Map<String, Subscriber> handlerMappings = new HashMap<String, Subscriber>();

    @Override
    public void put(Subscriber handler) {
        Objects.requireNonNull(handler, "handler");
        // 取得 Subscriber<T> 的实际消息类；多层继承交给已有工具处理。
        Class<?> eventClass = TypeUtils.getRawType(Subscriber.class.getTypeParameters()[0], handler.getClass());
        if (eventClass == null) {
            throw new IllegalArgumentException("Cannot resolve Subscriber message type; specify its Event class explicitly");
        }
        put(eventClass.asSubclass(Event.class), handler);
    }

    /** Registers a proxy using the message type resolved from its target class. */
    @Override
    public void put(Class<? extends Event> eventClass, Subscriber handler) {
        Objects.requireNonNull(eventClass, "eventClass");
        if (!Event.class.isAssignableFrom(eventClass)) {
            throw new IllegalArgumentException("Subscriber message type must implement Event");
        }
        handlerMappings.put(eventClass.getName(), Objects.requireNonNull(handler, "handler"));
    }

    @Override
    public Subscriber get(String eventClazzName) {
        if (this.handlerMappings != null) {
            return handlerMappings.get(eventClazzName);
        }
        return null;
    }
}
