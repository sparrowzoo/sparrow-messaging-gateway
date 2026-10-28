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


import com.sparrow.messaging.protocol.MessageType;
import com.sparrow.messaging.protocol.ProtocolType;
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
    private Map<ProtocolType, Map<Short, TypedSubscriber>> remoteSubscriberMapping = new HashMap<>();

    private Map<ProtocolType, TypedSubscriber> localSubscriberMapping = new HashMap<>();
    private Map<ProtocolType, Map<Short, TypedSubscriber>> localSubscriberMapping2 = new HashMap<>();

    @Override
    public void put(TypedSubscriber handler) {
        if (handler == null) {
            throw new IllegalArgumentException("Subscriber must not be null");
        }
        ProtocolType protocolType = handler.getProtocolType();
        if (protocolType == null) {
            throw new IllegalArgumentException("Subscriber must declare a protocol type");
        }
        MessageType messageType = handler.getMessageType();

        if (handler.localImmediately()) {
            if (messageType == null) {
                this.localSubscriberMapping.put(protocolType, handler);
            } else {
                this.localSubscriberMapping2.computeIfAbsent(protocolType, _ -> new HashMap<>());
                this.localSubscriberMapping2.get(protocolType).put(messageType.getIdentity().shortValue(), handler);
            }
            return;
        }
        if (messageType == null) {
            throw new IllegalArgumentException("Business subscriber must declare a message type");
        }
        Integer identity = messageType.getIdentity();
        if (identity == null || identity < 0 || identity > 65535) {
            throw new IllegalArgumentException("Message type identity must be between 0 and 65535");
        }
        this.remoteSubscriberMapping.computeIfAbsent(protocolType, ignored -> new HashMap<>())
                .put(identity.shortValue(), handler);
    }

    @Override
    public TypedSubscriber getImmediately(TypedEvent event) {
        ProtocolType protocolType = event.getProtocolType();
        short messageType = event.getMessageType();
        Map<Short, TypedSubscriber> messageTypeSubscriberMap = this.localSubscriberMapping2.get(protocolType);
        if (messageTypeSubscriberMap != null) {
            TypedSubscriber subscriber = messageTypeSubscriberMap.get(messageType);
            if (subscriber != null) {
                return subscriber;
            }
        }

        if (this.localSubscriberMapping != null) {
            return localSubscriberMapping.get(event.getProtocolType());
        }
        return null;
    }

    @Override
    public TypedSubscriber get(TypedEvent type) {
        if (this.remoteSubscriberMapping != null) {
            Map<Short, TypedSubscriber> subscriberMap = remoteSubscriberMapping.get(type.getProtocolType());
            if (subscriberMap != null) {
                return subscriberMap.get(type.getMessageType());
            }
        }
        return null;
    }
}
