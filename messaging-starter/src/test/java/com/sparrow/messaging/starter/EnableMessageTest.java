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
import com.sparrow.messaging.domain.service.PublishStrategy;
import com.sparrow.messaging.domain.service.RoutablePublisher;
import com.sparrow.messaging.domain.service.RoutableTypedHandlerMappingContainer;
import com.sparrow.messaging.infrastructure.service.spring.AbstractSpringTypedHandler;
import com.sparrow.messaging.infrastructure.service.spring.event.MessageEventHandler;
import com.sparrow.messaging.infrastructure.service.spring.event.PingHandler;
import com.sparrow.messaging.infrastructure.service.spring.event.UserStatusEventHandler;
import com.sparrow.messaging.protocol.EventType;
import com.sparrow.messaging.protocol.MessageType;
import com.sparrow.messaging.protocol.TypedEvent;
import com.sparrow.messaging.protocol.enums.DefaultEventType;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnableMessageTest {
    @Test
    void enablesComponentsAndRegistersEventHandlers() {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.register(Application.class);
            context.refresh();
            assertNotNull(context.getBean(ConnectionRegistry.class));
            assertNotNull(context.getBean(PublishStrategy.class));
            assertNotNull(context.getBean(RoutablePublisher.class));
            var mappings = context.getBean(RoutableTypedHandlerMappingContainer.class);
            assertSame(context.getBean(PingHandler.class), mappings.getEventHandler(event(DefaultEventType.PING)));
            assertInstanceOf(MessageEventHandler.class, mappings.getEventHandler(event(DefaultEventType.MESSAGE)));
            assertInstanceOf(UserStatusEventHandler.class, mappings.getEventHandler(event(DefaultEventType.USER_STATUS)));
        }
    }

    @Test
    void dispatchesMessagesToApplicationHandler() throws Throwable {
        try (var context = new AnnotationConfigApplicationContext(Application.class)) {
            context.getBean(PublishStrategy.class).publish(event(DefaultEventType.MESSAGE));
            assertTrue(context.getBean(ApplicationHandler.class).handled);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableMessage
    static class Application {
        @Bean
        ApplicationHandler applicationHandler() {
            return new ApplicationHandler();
        }
    }

    static class ApplicationHandler extends AbstractSpringTypedHandler<TypedEvent> {
        private boolean handled;

        @Override
        public void handle(TypedEvent event) {
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
        return new TypedEvent() {
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
