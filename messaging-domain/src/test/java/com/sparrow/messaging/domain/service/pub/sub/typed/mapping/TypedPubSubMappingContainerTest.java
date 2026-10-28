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
import com.sparrow.messaging.protocol.enums.DefaultProtocolType;
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
            public Boolean localImmediately() { return false; }
            public MessageType getMessageType() { return MissingIdentity.MESSAGE; }
            public ProtocolType getProtocolType() { return DefaultProtocolType.IM; }
        }));
    }

    @Test
    void eventSubscriberDoesNotNeedAMessageType() {
        var mappings = new TypedPubSubMappingContainer();
        var subscriber = new TypedSubscriber<Protocol>() {
            public void subscribe(Protocol event) {}
            public ProtocolType getProtocolType() { return DefaultProtocolType.PING; }
        };
        mappings.put(subscriber);
        var ping = new Protocol();ping.setProtocolType(DefaultProtocolType.PING);
        assertSame(subscriber, mappings.getImmediately(ping));
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
        var im = subscriber(DefaultProtocolType.IM, TestMessageType.TEXT);
        var ai = subscriber(DefaultProtocolType.AI, TestMessageType.TEXT);
        var imReply = subscriber(DefaultProtocolType.IM, null);
        mappings.put(im);
        mappings.put(ai);
        mappings.put(imReply);

        var message = new Protocol();
        message.setMessageType((short) 101);
        message.setProtocolType(DefaultProtocolType.IM);
        assertSame(im, mappings.get(message));
        assertSame(imReply, mappings.getImmediately(message));
        message.setProtocolType(DefaultProtocolType.AI);
        assertSame(ai, mappings.get(message));
        assertNull(mappings.getImmediately(message));
        message.setProtocolType(DefaultProtocolType.PING);
        assertNull(mappings.get(message));
    }

    @Test
    void invalidMessageNumbersHaveAnExplicitError() {
        var mappings = new TypedPubSubMappingContainer();
        for (var type : new TestMessageType[] {TestMessageType.NEGATIVE, TestMessageType.TOO_LARGE}) {
            assertThrows(IllegalArgumentException.class,
                () -> mappings.put(subscriber(DefaultProtocolType.IM, type)));
        }
    }

    @Test
    void unsignedMaximumMessageNumberCanBeLookedUp() {
        var mappings = new TypedPubSubMappingContainer();
        var handler = subscriber(DefaultProtocolType.IM, TestMessageType.HIGH);
        mappings.put(handler);
        var message = new Protocol();
        message.setProtocolType(DefaultProtocolType.IM);
        message.setMessageType((short) 65535);
        assertSame(handler, mappings.get(message));
    }

    @Test
    void businessSubscriberWithoutAMessageTypeIsRejected() {
        var mappings = new TypedPubSubMappingContainer();
        assertThrows(IllegalArgumentException.class, () -> mappings.put(new TypedSubscriber<Protocol>() {
            public ProtocolType getProtocolType() { return DefaultProtocolType.IM; }
            public Boolean localImmediately() { return false; }
            public void subscribe(Protocol event) {}
        }));
    }

    private TypedSubscriber<Protocol> subscriber(ProtocolType eventType, MessageType messageType) {
        return new TypedSubscriber<Protocol>() {
            public void subscribe(Protocol event) {}
            public ProtocolType getProtocolType() { return eventType; }
            public Boolean localImmediately() { return messageType == null; }
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
