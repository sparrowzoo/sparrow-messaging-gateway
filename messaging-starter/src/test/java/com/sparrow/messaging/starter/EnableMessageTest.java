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

package com.sparrow.messaging.starter;

import com.sparrow.messaging.domain.service.ConnectionRegistry;
import com.sparrow.messaging.domain.service.pub.sub.RoutingKeyPublisherMapping;
import com.sparrow.messaging.domain.service.pub.sub.typed.TypedPublishStrategy;
import com.sparrow.messaging.domain.service.pub.sub.typed.DefaultTypedPublisher;
import com.sparrow.messaging.domain.service.pub.sub.typed.mapping.TypedPubSubMappingContainer;
import com.sparrow.messaging.infrastructure.service.spring.AbstractSpringTypedSubscriber;
import com.sparrow.messaging.infrastructure.service.spring.event.IMEventSubscriber;
import com.sparrow.messaging.infrastructure.service.spring.event.PingSubscriber;
import com.sparrow.messaging.infrastructure.service.spring.event.UserStatusEventSubscriber;
import com.sparrow.messaging.protocol.EventType;
import com.sparrow.messaging.protocol.MessageType;
import com.sparrow.messaging.protocol.TypedEvent;
import com.sparrow.messaging.protocol.api.TypedPublisher;
import com.sparrow.messaging.protocol.enums.DefaultEventType;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.MapPropertySource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class EnableMessageTest {
    @Test
    void enablesComponentsAndRegistersEventHandlers() {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.register(Application.class);
            context.refresh();
            assertNotNull(context.getBean(ConnectionRegistry.class));
            assertNotNull(context.getBean(TypedPublishStrategy.class));
            assertNotNull(context.getBean(DefaultTypedPublisher.class));
            var mappings = context.getBean(TypedPubSubMappingContainer.class);
            assertSame(context.getBean(PingSubscriber.class), mappings.getEventHandler(event(DefaultEventType.PING)));
            assertInstanceOf(IMEventSubscriber.class, mappings.getEventHandler(event(DefaultEventType.IM)));
            assertInstanceOf(UserStatusEventSubscriber.class, mappings.getEventHandler(event(DefaultEventType.USER_STATUS)));
        }
    }

    @Test
    void dispatchesMessagesToApplicationHandler() throws Throwable {
        try (var context = new AnnotationConfigApplicationContext(Application.class)) {
            context.getBean(TypedPublishStrategy.class).publish(event(DefaultEventType.IM));
            assertTrue(context.getBean(ApplicationHandler.class).handled);
        }
    }

    @Test
    void mapsRoutingKeyToPublisherBeanName() throws Throwable {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("routing-test", Map.of(
                    "sparrow.massaging.routing-publisher.mapping",
                    "{'default':'defaultTypedPublisher','orders':'orderPublisher'}")));
            context.register(Application.class);
            context.refresh();
            var mapping = context.getBean(RoutingKeyPublisherMapping.class);
            assertEquals("defaultTypedPublisher", mapping.getPublisherKey("default"));
            assertEquals("orderPublisher", mapping.getPublisherKey("orders"));
            assertEquals("unmapped", mapping.getPublisherKey("unmapped"));
            TypedEvent order = event(DefaultEventType.IM, "orders");
            context.getBean(TypedPublishStrategy.class).publish(order);
            assertSame(order, context.getBean(CapturingPublisher.class).lastEvent);
            context.getBean(TypedPublishStrategy.class).publish(event(DefaultEventType.IM));
            assertTrue(context.getBean(ApplicationHandler.class).handled);
        }
    }

    @Test
    void keepsRoutingKeyWhenMappingIsNotConfigured() {
        try (var context = new AnnotationConfigApplicationContext(Application.class)) {
            assertEquals("orders", context.getBean(RoutingKeyPublisherMapping.class).getPublisherKey("orders"));
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableMessage
    static class Application {
        @Bean
        CapturingPublisher orderPublisher() {
            return new CapturingPublisher();
        }

        @Bean
        ApplicationHandler applicationHandler() {
            return new ApplicationHandler();
        }
    }

    static class CapturingPublisher implements TypedPublisher {
        private TypedEvent lastEvent;

        @Override
        public void publish(TypedEvent event) {
            this.lastEvent = event;
        }
    }

    static class ApplicationHandler extends AbstractSpringTypedSubscriber<TypedEvent> {
        private boolean handled;

        @Override
        public void subscribe(TypedEvent event) {
            this.handled = true;
        }

        @Override
        public MessageType getMessageType() {
            return TestMessageType.MESSAGE;
        }
    }

    enum TestMessageType implements MessageType {
        MESSAGE;

        @Override
        public Integer getIdentity() {
            return 0;
        }
    }

    private static TypedEvent event(EventType type) {
        return event(type, "default");
    }

    private static TypedEvent event(EventType type, String routingKey) {
        return new TypedEvent() {
            @Override
            public String getRoutingKey() {
                return routingKey;
            }

            @Override
            public short getMessageType() {
                return 0;
            }

            @Override
            public EventType getEventType() {
                return type;
            }
        };
    }
}
