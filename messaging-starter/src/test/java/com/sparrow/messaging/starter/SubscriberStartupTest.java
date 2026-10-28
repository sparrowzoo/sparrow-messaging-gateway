/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sparrow.messaging.starter;

import com.sparrow.messaging.domain.service.pub.sub.mapping.PubSubMappingContainer;
import com.sparrow.messaging.domain.service.pub.sub.typed.mapping.TypedPubSubMappingContainer;
import com.sparrow.messaging.protocol.Event;
import com.sparrow.messaging.protocol.Protocol;
import com.sparrow.messaging.protocol.api.Subscriber;
import com.sparrow.messaging.protocol.enums.DefaultProtocolType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.SmartLifecycle;
import org.springframework.aop.framework.ProxyFactory;
import org.aopalliance.intercept.MethodInterceptor;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SubscriberStartupTest {
    @Test
    void asynchronousApplicationEventsDoNotDelayRegistration() {
        var callbacks = new ArrayList<Runnable>();
        var notified = new AtomicInteger();
        try (var context = new AnnotationConfigApplicationContext()) {
            var multicaster = new SimpleApplicationEventMulticaster(context.getBeanFactory());
            multicaster.setTaskExecutor(callbacks::add);
            context.getBeanFactory().registerSingleton(
                    AbstractApplicationContext.APPLICATION_EVENT_MULTICASTER_BEAN_NAME, multicaster);
            context.addApplicationListener(event -> {
                if (event instanceof ContextRefreshedEvent) {
                    notified.incrementAndGet();
                }
            });
            context.register(MessageConfiguration.class);
            context.refresh();
            assertNotNull(context.getBean(TypedPubSubMappingContainer.class).getImmediately(ping()));
            assertEquals(0, notified.get(), "Unrelated application events must remain asynchronous");
            new ArrayList<>(callbacks).forEach(Runnable::run);
            assertEquals(1, notified.get());
        }
    }

    @Test
    void ordinaryAndTypedSubscribersAreReadyBeforeLifecycleStart() {
        var starts = new AtomicInteger();
        try (var context = new AnnotationConfigApplicationContext()) {
            context.register(MessageConfiguration.class);
            context.registerBean("ordinary", SubscriberProxyTest.PlainSubscriber.class,
                    () -> new SubscriberProxyTest.PlainSubscriber(new AtomicInteger()));
            context.registerBean("typed", SubscriberProxyTest.SecuredTypedSubscriber.class,
                    () -> new SubscriberProxyTest.SecuredTypedSubscriber(new AtomicInteger()));
            context.registerBean("businessService", SmartLifecycle.class, () -> new BusinessLifecycle(() -> {
                assertSame(context.getBean("ordinary"), context.getBean(PubSubMappingContainer.class)
                        .get(SubscriberProxyTest.PlainEvent.class.getName()));
                assertSame(context.getBean("typed"), context.getBean(TypedPubSubMappingContainer.class)
                        .get(new SubscriberProxyTest.SampleTypedEvent()));
                assertNotNull(context.getBean(TypedPubSubMappingContainer.class).getImmediately(ping()));
                starts.incrementAndGet();
            }));
            context.refresh();
            assertEquals(1, starts.get());
        }
    }

    @Test
    void lazySubscribersAreNotCreatedOrRegisteredByTheFramework() {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.register(MessageConfiguration.class);
            context.registerBean("ordinary", SubscriberProxyTest.PlainSubscriber.class,
                    () -> new SubscriberProxyTest.PlainSubscriber(new AtomicInteger()), bean -> bean.setLazyInit(true));
            context.registerBean("typed", SubscriberProxyTest.SecuredTypedSubscriber.class,
                    () -> new SubscriberProxyTest.SecuredTypedSubscriber(new AtomicInteger()), bean -> bean.setLazyInit(true));
            context.refresh();
            assertFalse(context.getBeanFactory().containsSingleton("ordinary"));
            assertFalse(context.getBeanFactory().containsSingleton("typed"));
            assertNull(context.getBean(PubSubMappingContainer.class)
                    .get(SubscriberProxyTest.PlainEvent.class.getName()));
            assertNull(context.getBean(TypedPubSubMappingContainer.class)
                    .get(new SubscriberProxyTest.SampleTypedEvent()));
        }
    }

    @Test
    void jdkAndClassProxiesKeepAuthorizationBeforeLifecycleStart() {
        for (boolean typed : new boolean[]{false, true}) {
            for (boolean classProxy : new boolean[]{false, true}) {
                var executions = new AtomicInteger();
                try (var context = new AnnotationConfigApplicationContext()) {
                    context.getBeanFactory().addBeanPostProcessor(new BeanPostProcessor() {
                        public Object postProcessAfterInitialization(Object bean, String name) {
                            if (!"secured".equals(name)) {
                                return bean;
                            }
                            var proxy = new ProxyFactory(bean);
                            proxy.setProxyTargetClass(classProxy);
                            proxy.addAdvice((MethodInterceptor) invocation -> {
                                if (invocation.getMethod().getName().equals("subscribe")) {
                                    throw new SecurityException("Denied");
                                }
                                return invocation.proceed();
                            });
                            return proxy.getProxy();
                        }
                    });
                    context.register(MessageConfiguration.class);
                    if (typed) {
                        context.registerBean("secured", SubscriberProxyTest.SecuredTypedSubscriber.class,
                                () -> new SubscriberProxyTest.SecuredTypedSubscriber(executions));
                    } else {
                        context.registerBean("secured", SubscriberProxyTest.PlainSubscriber.class,
                                () -> new SubscriberProxyTest.PlainSubscriber(executions));
                    }
                    context.registerBean("businessService", SmartLifecycle.class, () -> new BusinessLifecycle(() -> {
                        Subscriber registered = typed
                                ? context.getBean(TypedPubSubMappingContainer.class)
                                    .get(new SubscriberProxyTest.SampleTypedEvent())
                                : context.getBean(PubSubMappingContainer.class)
                                    .get(SubscriberProxyTest.PlainEvent.class.getName());
                        assertSame(context.getBean("secured"), registered);
                        assertThrows(SecurityException.class, () -> registered.subscribe(typed
                                ? new SubscriberProxyTest.SampleTypedEvent() : new SubscriberProxyTest.PlainEvent()));
                        assertEquals(0, executions.get());
                    }));
                    context.refresh();
                }
            }
        }
    }

    @Test
    void applicationInitializationCallbacksAreNotInvokedTwice() {
        var initialized = new AtomicInteger();
        var subclassInitialized = new AtomicInteger();
        try (var context = new AnnotationConfigApplicationContext()) {
            context.register(MessageConfiguration.class);
            context.registerBean(IndependentSubscriber.class, () -> new IndependentSubscriber(initialized));
            context.registerBean(InitializationAwareSubscriber.class,
                    () -> new InitializationAwareSubscriber(subclassInitialized));
            context.refresh();
            assertEquals(1, initialized.get());
            assertEquals(1, subclassInitialized.get());
        }
    }

    @Test
    void manuallyConfiguredEagerSubscribersStillRegisterThemselves() {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.registerBean(PubSubMappingContainer.class);
            context.registerBean(TypedPubSubMappingContainer.class);
            context.registerBean("ordinary", SubscriberProxyTest.PlainSubscriber.class,
                    () -> new SubscriberProxyTest.PlainSubscriber(new AtomicInteger()));
            context.registerBean("typed", SubscriberProxyTest.SecuredTypedSubscriber.class,
                    () -> new SubscriberProxyTest.SecuredTypedSubscriber(new AtomicInteger()));
            context.refresh();
            assertSame(context.getBean("ordinary"), context.getBean(PubSubMappingContainer.class)
                    .get(SubscriberProxyTest.PlainEvent.class.getName()));
            assertSame(context.getBean("typed"), context.getBean(TypedPubSubMappingContainer.class)
                    .get(new SubscriberProxyTest.SampleTypedEvent()));
        }
    }

    @Test
    void childContextDoesNotReplaceParentSubscription() {
        try (var parent = new AnnotationConfigApplicationContext();
             var child = new AnnotationConfigApplicationContext()) {
            parent.register(MessageConfiguration.class);
            parent.registerBean("ordinary", SubscriberProxyTest.PlainSubscriber.class,
                    () -> new SubscriberProxyTest.PlainSubscriber(new AtomicInteger()));
            parent.refresh();
            child.setParent(parent);
            child.register(MessageConfiguration.class);
            child.registerBean("ordinary", SubscriberProxyTest.PlainSubscriber.class,
                    () -> new SubscriberProxyTest.PlainSubscriber(new AtomicInteger()));
            child.refresh();
            assertSame(parent.getBean("ordinary"), parent.getBean(PubSubMappingContainer.class)
                    .get(SubscriberProxyTest.PlainEvent.class.getName()));
            assertSame(child.getBean("ordinary"), child.getBean(PubSubMappingContainer.class)
                    .get(SubscriberProxyTest.PlainEvent.class.getName()));
        }
    }

    private static Protocol ping() {
        var ping = new Protocol();
        ping.setProtocolType(DefaultProtocolType.PING);
        return ping;
    }

    static class BusinessLifecycle implements SmartLifecycle {
        private final Runnable onStart;
        private boolean running;
        BusinessLifecycle(Runnable onStart) { this.onStart = onStart; }
        public void start() { onStart.run(); running = true; }
        public void stop() { running = false; }
        public boolean isRunning() { return running; }
    }

    static class IndependentSubscriber implements Subscriber<Event>, SmartInitializingSingleton {
        private final AtomicInteger initialized;
        IndependentSubscriber(AtomicInteger initialized) { this.initialized = initialized; }
        public void afterSingletonsInstantiated() { initialized.incrementAndGet(); }
        public void subscribe(Event event) {}
    }

    static class InitializationAwareSubscriber extends SubscriberProxyTest.PlainSubscriber {
        private final AtomicInteger initialized;
        InitializationAwareSubscriber(AtomicInteger initialized) {
            super(new AtomicInteger());
            this.initialized = initialized;
        }
        @Override
        public void afterSingletonsInstantiated() {
            initialized.incrementAndGet();
            super.afterSingletonsInstantiated();
        }
    }

}
