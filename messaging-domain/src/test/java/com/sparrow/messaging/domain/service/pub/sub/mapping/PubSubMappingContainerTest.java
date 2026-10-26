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

import com.sparrow.messaging.protocol.Event;
import com.sparrow.messaging.protocol.api.AbstractSubHandler;
import com.sparrow.messaging.protocol.api.Subscriber;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PubSubMappingContainerTest {
    @Test
    void registersSubscriberThroughNonGenericIntermediateClass() {
        assertRegisters(new Actual());
    }

    @Test
    void registersDirectSubscriberImplementation() {
        assertRegisters(new Direct());
    }

    @Test
    void followsGenericInterfaceBindings() {
        assertRegisters(new ThroughInterface());
    }

    @Test
    void followsTypeVariableAcrossSeveralParents() {
        assertRegisters(new Bound());
    }

    @Test
    void rejectsUnboundTypeRatherThanRegisteringEventUpperBound() {
        var mappings = new PubSubMappingContainer();
        assertThrows(IllegalArgumentException.class, () -> mappings.put(new Generic<>()));
        assertNull(mappings.get(Event.class.getName()));
    }

    @Test
    void rejectsRawSubscriberWithReadableError() {
        var exception = assertThrows(IllegalArgumentException.class,
                () -> new PubSubMappingContainer().put(new Raw()));
        assertTrue(exception.getMessage().contains("Subscriber"));
    }

    private void assertRegisters(Subscriber<Message> subscriber) {
        var mappings = new PubSubMappingContainer();
        assertDoesNotThrow(() -> mappings.put(subscriber));
        assertSame(subscriber, mappings.get(Message.class.getName()));
    }

    static class Message implements Event {
    }

    static class Base extends AbstractSubHandler<Message> {
        @Override
        public void subscribe(Message event) {
        }
    }

    static class Actual extends Base {
    }

    static class Direct implements Subscriber<Message> {
        @Override
        public void subscribe(Message event) {
        }
    }

    interface Intermediate<T extends Event> extends Subscriber<T> {
    }

    static class ThroughInterface implements Intermediate<Message> {
        @Override
        public void subscribe(Message event) {
        }
    }

    static class Generic<T extends Event> extends AbstractSubHandler<T> {
        @Override
        public void subscribe(T event) {
        }
    }

    static class Middle<U extends Event> extends Generic<U> {
    }

    static class Bound extends Middle<Message> {
    }

    @SuppressWarnings("rawtypes")
    static class Raw implements Subscriber {
        @Override
        public void subscribe(Event event) {
        }
    }
}
