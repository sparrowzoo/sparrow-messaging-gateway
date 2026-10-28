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
import com.sparrow.messaging.domain.service.pub.sub.PublishStrategy;
import com.sparrow.messaging.domain.service.pub.sub.RoutingKeyPublisherMapping;
import com.sparrow.messaging.domain.service.pub.sub.typed.TypedPublishStrategy;
import com.sparrow.messaging.domain.service.pub.sub.typed.DefaultTypedPublisher;
import com.sparrow.messaging.domain.service.pub.sub.typed.mapping.TypedPubSubMappingContainer;
import com.sparrow.messaging.infrastructure.service.spring.AbstractSpringTypedSubscriber;
import com.sparrow.messaging.infrastructure.service.spring.event.IMEventSubscriber;
import com.sparrow.messaging.infrastructure.service.spring.event.AIEventSubscriber;
import com.sparrow.messaging.infrastructure.service.spring.event.PingSubscriber;
import com.sparrow.messaging.infrastructure.service.spring.event.UserStatusEventSubscriber;
import com.sparrow.messaging.protocol.ProtocolType;
import com.sparrow.messaging.protocol.MessageType;
import com.sparrow.messaging.protocol.TypedEvent;
import com.sparrow.messaging.protocol.api.TypedPublisher;
import com.sparrow.messaging.protocol.enums.DefaultProtocolType;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import org.springframework.core.env.MapPropertySource;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class EnableMessageTest {
    @Test
    void dispatchesOrdinaryEventsThroughPublishStrategy() throws Throwable {
        var received = new AtomicInteger();
        try (var context = new AnnotationConfigApplicationContext()) {
            context.register(Application.class);
            context.registerBean("ordinarySubscriber", SubscriberProxyTest.PlainSubscriber.class,
                    () -> new SubscriberProxyTest.PlainSubscriber(received));
            context.refresh();
            context.getBean(PublishStrategy.class).publish(new SubscriberProxyTest.PlainEvent());
            assertEquals(1, received.get());
        }
    }

    @Test
    void enablesComponentsAndRegistersEventHandlers() {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.register(Application.class);
            context.refresh();
            assertNotNull(context.getBean(ConnectionRegistry.class));
            assertNotNull(context.getBean(TypedPublishStrategy.class));
            assertNotNull(context.getBean(DefaultTypedPublisher.class));
            var mappings = context.getBean(TypedPubSubMappingContainer.class);
            assertSame(context.getBean(PingSubscriber.class), mappings.getImmediately(event(DefaultProtocolType.PING)));
            assertInstanceOf(IMEventSubscriber.class, mappings.getImmediately(event(DefaultProtocolType.IM)));
            assertInstanceOf(AIEventSubscriber.class, mappings.getImmediately(event(DefaultProtocolType.AI)));
            assertInstanceOf(UserStatusEventSubscriber.class, mappings.getImmediately(event(DefaultProtocolType.FETCH_USER_STATUS)));
        }
    }

    @Test
    void dispatchesMessagesToApplicationHandler() throws Throwable {
        try (var context = new AnnotationConfigApplicationContext(Application.class)) {
            context.getBean(TypedPublishStrategy.class).publish(event(DefaultProtocolType.IM));
            assertTrue(context.getBean(ApplicationHandler.class).handled);
        }
    }


    @Test
    void typedRoutingUsesConfiguredPublisherWithoutAProtocolSuffix() throws Throwable {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("routing-test", Map.of(
                    "sparrow.massaging.routing-publisher.mapping", "{'orders':'orderPublisher'}")));
            context.register(Application.class);
            context.refresh();
            TypedEvent order = event(DefaultProtocolType.IM, "orders");
            context.getBean(TypedPublishStrategy.class).publish(order);
            assertSame(order, context.getBean(CapturingPublisher.class).lastEvent);
        }
    }

    @Test
    void protocolSpecificRoutingTakesPrecedenceOverTheBaseRoute() {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("routing-test", Map.of(
                    "sparrow.massaging.routing-publisher.mapping",
                    "{'orders':'sharedPublisher','orders_IM':'orderPublisher'}")));
            context.register(Application.class);
            context.refresh();
            var mapping = context.getBean(RoutingKeyPublisherMapping.class);
            assertEquals("orderPublisher", mapping.getPublisherKey(event(DefaultProtocolType.IM, "orders")));
            assertEquals("sharedPublisher", mapping.getPublisherKey(event(DefaultProtocolType.AI, "orders")));
            assertEquals("unmapped", mapping.getPublisherKey(event(DefaultProtocolType.IM, "unmapped")));
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
        public ProtocolType getProtocolType() {
            return DefaultProtocolType.IM;
        }

        @Override
        public void subscribe(TypedEvent event) {
            this.handled = true;
        }

        @Override
        public Boolean localImmediately() {
            return false;
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

    private static TypedEvent event(ProtocolType type) {
        return event(type, "default");
    }

    private static TypedEvent event(ProtocolType type, String routingKey) {
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
            public ProtocolType getProtocolType() {
                return type;
            }
        };
    }
}
