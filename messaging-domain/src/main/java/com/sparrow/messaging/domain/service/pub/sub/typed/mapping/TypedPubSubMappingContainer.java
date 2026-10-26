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

package com.sparrow.messaging.domain.service.pub.sub.typed.mapping;


import com.sparrow.messaging.protocol.EventType;
import com.sparrow.messaging.protocol.MessageType;
import com.sparrow.messaging.protocol.TypedEvent;
import com.sparrow.messaging.protocol.api.EventTypedHandlerMappingContainer;
import com.sparrow.messaging.protocol.api.TypedSubscriber;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor
public class TypedPubSubMappingContainer implements EventTypedHandlerMappingContainer {
    /**
     * {@code map<EventName,MQHandler>}
     */
    private Map<Short, TypedSubscriber> messageTypeHandlerMapping = new HashMap<>();

    private Map<EventType, TypedSubscriber> eventTypeHandlerMapping = new HashMap<>();

    @Override
    public void put(TypedSubscriber handler) {
        if (handler == null) {
            throw new IllegalArgumentException("Subscriber must not be null");
        }
        EventType eventType = handler.getEventType();
        if (eventType == null) {
            MessageType messageType = handler.getMessageType();
            if (messageType == null || messageType.getIdentity() == null) {
                throw new IllegalArgumentException("Subscriber must declare an event type or a message type with an identity");
            }
            this.messageTypeHandlerMapping.put(messageType.getIdentity().shortValue(), handler);
            return;
        }
        this.eventTypeHandlerMapping.put(eventType, handler);
    }

    @Override
    public TypedSubscriber getEventHandler(TypedEvent event) {
        if (this.eventTypeHandlerMapping != null) {
            return eventTypeHandlerMapping.get(event.getEventType());
        }
        return null;
    }

    @Override
    public TypedSubscriber getMessageHandler(TypedEvent type) {
        if (this.messageTypeHandlerMapping != null) {
            return messageTypeHandlerMapping.get(type.getMessageType());
        }
        return null;
    }
}
