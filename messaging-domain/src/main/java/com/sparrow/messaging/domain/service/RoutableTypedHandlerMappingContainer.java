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


import com.sparrow.messaging.protocol.EventType;
import com.sparrow.messaging.protocol.MessageType;
import com.sparrow.messaging.protocol.TypedEvent;
import com.sparrow.messaging.protocol.api.EventTypedHandlerMappingContainer;
import com.sparrow.messaging.protocol.api.TypedSubHandler;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor
public class RoutableTypedHandlerMappingContainer implements EventTypedHandlerMappingContainer {
    /**
     * {@code map<EventName,MQHandler>}
     */
    private Map<String, Map<Short, TypedSubHandler>> messageTypeHandlerMapping = new HashMap<String, Map<Short, TypedSubHandler>>();

    private Map<String, Map<EventType, TypedSubHandler>> eventTypeHandlerMapping = new HashMap<>();

    @Override
    public void put(TypedSubHandler handler) {
        EventType eventType = handler.getEventType();
        if (eventType == null) {
            MessageType messageType = handler.getMessageType();
            this.messageTypeHandlerMapping.computeIfAbsent(handler.getRoutingKey(), k -> new HashMap<>());
            this.messageTypeHandlerMapping.get(handler.getRoutingKey()).put(messageType.getIdentity().shortValue(), handler);
            return;
        }

        this.eventTypeHandlerMapping.computeIfAbsent(handler.getRoutingKey(), k -> new HashMap<>());
        this.eventTypeHandlerMapping.get(handler.getRoutingKey()).put(eventType, handler);
    }

    @Override
    public TypedSubHandler getEventHandler(TypedEvent event) {
        if (this.eventTypeHandlerMapping != null) {
            Map<EventType, TypedSubHandler> eventTypeTypedSubHandlerMap = this.eventTypeHandlerMapping.get(event.getRoutingKey());
            if (eventTypeTypedSubHandlerMap != null) {
                return eventTypeTypedSubHandlerMap.get(event.getEventType());
            }
        }
        return null;
    }

    @Override
    public TypedSubHandler getMessageHandler(TypedEvent type) {
        if (this.messageTypeHandlerMapping != null) {
            Map<Short, TypedSubHandler> massageHandlerMapping = this.messageTypeHandlerMapping.get(type.getRoutingKey());
            if (massageHandlerMapping != null) {
                return massageHandlerMapping.get(type.getMessageType());
            }
        }
        return null;
    }
}
