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
            public EventType getEventType() { return DefaultEventType.IM; }
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

    @Test
    void messageSubscriberMustDeclareItsEventType() {
        var mappings = new TypedPubSubMappingContainer();
        assertThrows(IllegalArgumentException.class,
            () -> mappings.put(subscriber(null, TestMessageType.TEXT)));
    }

    @Test
    void sameMessageNumberInDifferentEventTypesUsesIndependentSubscribers() {
        var mappings = new TypedPubSubMappingContainer();
        var im = subscriber(DefaultEventType.IM, TestMessageType.TEXT);
        var ai = subscriber(DefaultEventType.AI, TestMessageType.TEXT);
        var imReply = subscriber(DefaultEventType.IM, null);
        mappings.put(im);
        mappings.put(ai);
        mappings.put(imReply);

        var message = new Protocol();
        message.setMessageType((short) 101);
        message.setEventType(DefaultEventType.IM);
        assertSame(im, mappings.getMessageHandler(message));
        assertSame(imReply, mappings.getEventHandler(message));
        message.setEventType(DefaultEventType.AI);
        assertSame(ai, mappings.getMessageHandler(message));
        assertNull(mappings.getEventHandler(message));
        message.setEventType(DefaultEventType.PING);
        assertNull(mappings.getMessageHandler(message));
    }

    @Test
    void invalidMessageNumbersHaveAnExplicitError() {
        var mappings = new TypedPubSubMappingContainer();
        for (var type : new TestMessageType[] {TestMessageType.NEGATIVE, TestMessageType.TOO_LARGE}) {
            assertThrows(IllegalArgumentException.class,
                () -> mappings.put(subscriber(DefaultEventType.IM, type)));
        }
    }

    @Test
    void unsignedMaximumMessageNumberCanBeLookedUp() {
        var mappings = new TypedPubSubMappingContainer();
        var handler = subscriber(DefaultEventType.IM, TestMessageType.HIGH);
        mappings.put(handler);
        var message = new Protocol();
        message.setEventType(DefaultEventType.IM);
        message.setMessageType((short) 65535);
        assertSame(handler, mappings.getMessageHandler(message));
    }

    private TypedSubscriber<Protocol> subscriber(EventType eventType, MessageType messageType) {
        return new TypedSubscriber<Protocol>() {
            public void subscribe(Protocol event) {}
            public EventType getEventType() { return eventType; }
            public MessageType getMessageType() { return messageType; }
        };
    }

    enum TestMessageType implements MessageType {
        TEXT(101), NEGATIVE(-1), TOO_LARGE(65536), HIGH(65535);

        private final Integer identity;
        TestMessageType(Integer identity) { this.identity = identity; }
        public Integer getIdentity() { return identity; }
    }

    enum MissingIdentity implements MessageType {
        MESSAGE;
        public Integer getIdentity() { return null; }
    }
}
