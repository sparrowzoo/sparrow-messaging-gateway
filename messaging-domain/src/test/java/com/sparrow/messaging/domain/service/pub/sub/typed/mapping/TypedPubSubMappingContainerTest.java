/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.sparrow.messaging.domain.service.pub.sub.typed.mapping;

import com.sparrow.messaging.protocol.*;
import com.sparrow.messaging.protocol.api.TypedSubscriber;
import com.sparrow.messaging.protocol.enums.DefaultEventType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TypedPubSubMappingContainerTest {
    @Test
    void missingSubscriberOrMessageDeclarationHasAnExplicitError() {
        var mappings = new TypedPubSubMappingContainer();
        assertThrows(IllegalArgumentException.class, () -> mappings.put(null));
        assertThrows(IllegalArgumentException.class, () -> mappings.put(new TypedSubscriber<Protocol>() {
            public void subscribe(Protocol event) {}
        }));
    }

    @Test
    void missingMessageIdentityHasAnExplicitError() {
        var mappings = new TypedPubSubMappingContainer();
        assertThrows(IllegalArgumentException.class, () -> mappings.put(new TypedSubscriber<Protocol>() {
            public void subscribe(Protocol event) {}
            public MessageType getMessageType() { return MissingIdentity.MESSAGE; }
        }));
    }

    @Test
    void eventSubscriberDoesNotNeedAMessageType() {
        var mappings = new TypedPubSubMappingContainer();
        var subscriber = new TypedSubscriber<Protocol>() {
            public void subscribe(Protocol event) {}
            public EventType getEventType() { return DefaultEventType.PING; }
        };
        mappings.put(subscriber);
        var ping = new Protocol();ping.setEventType(DefaultEventType.PING);
        assertSame(subscriber, mappings.getEventHandler(ping));
    }

    enum MissingIdentity implements MessageType {
        MESSAGE;
        public Integer getIdentity() { return null; }
    }
}
