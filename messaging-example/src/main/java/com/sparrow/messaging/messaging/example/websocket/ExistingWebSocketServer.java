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

package com.sparrow.messaging.messaging.example.websocket;

import com.sparrow.authenticator.Authenticator;
import com.sparrow.container.ConfigReader;
import com.sparrow.container.FactoryBean;
import com.sparrow.container.config.SparrowConfigReader;
import com.sparrow.core.spi.ApplicationContext;
import com.sparrow.messaging.domain.service.ConnectionRegistry;
import com.sparrow.messaging.domain.service.pub.sub.typed.TypedPublishStrategy;
import com.sparrow.messaging.domain.service.pub.sub.typed.mapping.TypedPubSubMappingContainer;
import com.sparrow.messaging.infrastructure.service.netty.WebSocketServer;
import com.sparrow.utility.ClassUtility;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 启动项目现有服务器，并把示例的 Spring Bean 登记到现有 Sparrow 容器。
 */
public class ExistingWebSocketServer implements AutoCloseable {
    private final FactoryBean<Object> registry;
    private final Map<String, Object> previousBeans = new LinkedHashMap<>();
    private final String previousPort = System.getProperty("port");
    private final String previousHost = System.getProperty("host");
    private final AtomicReference<Exception> failure = new AtomicReference<>();
    private final Thread serverThread;
    private final int port;
    private volatile boolean closing;

    @SuppressWarnings("unchecked")
    public ExistingWebSocketServer(org.springframework.context.ApplicationContext context) throws Exception {
        registry = ApplicationContext.getContainer().getSingletonRegister();
        try (var reservation = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
            port = reservation.getLocalPort();
        }
        bind(ConfigReader.class, new SparrowConfigReader());
        bind(Authenticator.class, context.getBean(Authenticator.class));
        bind(ConnectionRegistry.class, context.getBean(ConnectionRegistry.class));
        bind(TypedPubSubMappingContainer.class, context.getBean(TypedPubSubMappingContainer.class));
        bind(TypedPublishStrategy.class, context.getBean(TypedPublishStrategy.class));
        System.setProperty("port", Integer.toString(port));
        System.setProperty("host", "127.0.0.1");
        serverThread = Thread.ofPlatform().name("existing-netty-websocket-server").daemon().unstarted(() -> {
            try {
                WebSocketServer.start(new String[0]);
            } catch (Exception exception) {
                if (!closing) {
                    failure.set(exception);
                }
            }
        });
        serverThread.start();
        try {
            awaitListening();
        } catch (Exception exception) {
            close();
            throw exception;
        }
    }

    private void bind(Class<?> type, Object bean) {
        String name = ClassUtility.getBeanNameByClass(type);
        previousBeans.put(name, registry.getObject(name));
        registry.pubObject(name, bean);
    }

    private void awaitListening() throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            if (failure.get() != null) {
                throw failure.get();
            }
            try (var socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", port), 100);
                return;
            } catch (IOException ignored) {
                Thread.sleep(25);
            }
        }
        throw new IllegalStateException("现有 WebSocketServer 启动超时");
    }

    public URI uri() {
        return URI.create("ws://127.0.0.1:" + port + "/websocket");
    }

    @Override
    public void close() throws InterruptedException {
        closing = true;
        serverThread.interrupt();
        serverThread.join(10000);
        if (serverThread.isAlive()) {
            throw new IllegalStateException("现有 WebSocketServer 未能正常退出");
        }
        for (var entry : previousBeans.entrySet()) {
            if (entry.getValue() == null) {
                registry.removeObject(entry.getKey());
            } else {
                registry.pubObject(entry.getKey(), entry.getValue());
            }
        }
        restoreProperty("port", previousPort);
        restoreProperty("host", previousHost);
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}
