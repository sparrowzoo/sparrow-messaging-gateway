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

package com.sparrow.messaging.starter;

import com.sparrow.messaging.domain.service.pub.sub.mapping.PubSubMappingContainer;
import com.sparrow.messaging.domain.service.pub.sub.typed.mapping.TypedPubSubMappingContainer;
import com.sparrow.messaging.infrastructure.service.spring.AbstractSpringSubscriber;
import com.sparrow.messaging.infrastructure.service.spring.AbstractSpringTypedSubscriber;
import com.sparrow.messaging.protocol.Event;
import com.sparrow.messaging.protocol.EventType;
import com.sparrow.messaging.protocol.MessageType;
import com.sparrow.messaging.protocol.TypedEvent;
import com.sparrow.messaging.protocol.api.Subscriber;
import com.sparrow.messaging.protocol.enums.DefaultEventType;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SubscriberProxyTest {
    @Test
    void ordinaryJdkProxyKeepsSubscribeAuthorization() throws Throwable {
        verifiesAuthorization(false, false);
    }

    @Test
    void ordinaryClassProxyKeepsSubscribeAuthorization() throws Throwable {
        verifiesAuthorization(false, true);
    }

    @Test
    void typedJdkProxyKeepsSubscribeAuthorization() throws Throwable {
        verifiesAuthorization(true, false);
    }

    @Test
    void typedClassProxyKeepsSubscribeAuthorization() throws Throwable {
        verifiesAuthorization(true, true);
    }

    @Test
    void ordinarySubscriberIsInitializedAndRegistered() throws Throwable {
        AtomicInteger executions = new AtomicInteger();
        try (var context = new AnnotationConfigApplicationContext()) {
            context.register(MessageConfiguration.class);
            context.registerBean("ordinarySubscriber", PlainSubscriber.class,
                    () -> new PlainSubscriber(executions));
            context.refresh();
            Subscriber subscriber = context.getBean(PubSubMappingContainer.class).get(PlainEvent.class.getName());
            assertNotNull(subscriber);
            subscriber.subscribe(new PlainEvent());
            assertEquals(1, executions.get());
        }
    }

    private void verifiesAuthorization(boolean typed, boolean classProxy) throws Throwable {
        AtomicInteger executions = new AtomicInteger();
        try (var context = new AnnotationConfigApplicationContext()) {
            context.getBeanFactory().addBeanPostProcessor(new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String name) {
                    if (!"securedSubscriber".equals(name)) {
                        return bean;
                    }
                    ProxyFactory factory = new ProxyFactory(bean);
                    factory.setProxyTargetClass(classProxy);
                    factory.addAdvice((MethodInterceptor) invocation -> {
                        if (invocation.getMethod().getName().equals("subscribe")) {
                            throw new SecurityException("Subscription denied");
                        }
                        return invocation.proceed();
                    });
                    return factory.getProxy();
                }
            });
            context.register(MessageConfiguration.class);
            if (typed) {
                context.registerBean("securedSubscriber", SecuredTypedSubscriber.class,
                        () -> new SecuredTypedSubscriber(executions));
            } else {
                context.registerBean("securedSubscriber", PlainSubscriber.class,
                        () -> new PlainSubscriber(executions));
            }
            context.refresh();
            Subscriber subscriber = typed
                    ? context.getBean(TypedPubSubMappingContainer.class).getMessageHandler(new SampleTypedEvent())
                    : context.getBean(PubSubMappingContainer.class).get(PlainEvent.class.getName());
            assertNotNull(subscriber);
            assertSame(context.getBean("securedSubscriber"), subscriber);
            assertThrows(SecurityException.class,
                    () -> subscriber.subscribe(typed ? new SampleTypedEvent() : new PlainEvent()));
            assertEquals(0, executions.get());
        }
    }

    public static class PlainEvent implements Event {
    }

    public static class SampleTypedEvent implements TypedEvent {
        @Override
        public short getMessageType() {
            return 401;
        }

        @Override
        public EventType getEventType() {
            return DefaultEventType.IM;
        }
    }

    public static class PlainSubscriber extends AbstractSpringSubscriber<PlainEvent> {
        private final AtomicInteger executions;

        PlainSubscriber(AtomicInteger executions) {
            this.executions = executions;
        }

        @Override
        public void subscribe(PlainEvent event) {
            executions.incrementAndGet();
        }
    }

    public static class SecuredTypedSubscriber extends AbstractSpringTypedSubscriber<SampleTypedEvent> {
        private final AtomicInteger executions;

        SecuredTypedSubscriber(AtomicInteger executions) {
            this.executions = executions;
        }

        @Override
        public MessageType getMessageType() {
            return SampleMessageType.SAMPLE;
        }

        @Override
        public void subscribe(SampleTypedEvent event) {
            executions.incrementAndGet();
        }
    }

    enum SampleMessageType implements MessageType {
        SAMPLE;

        @Override
        public Integer getIdentity() {
            return 401;
        }
    }
}
