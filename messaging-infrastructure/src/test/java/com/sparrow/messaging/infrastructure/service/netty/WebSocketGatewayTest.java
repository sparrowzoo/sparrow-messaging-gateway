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
package com.sparrow.messaging.infrastructure.service.netty;

import com.sparrow.authenticator.DefaultLoginUser;
import com.sparrow.container.ConfigReader;
import com.sparrow.container.FactoryBean;
import com.sparrow.container.config.SparrowConfigReader;
import com.sparrow.core.spi.ApplicationContext;
import com.sparrow.messaging.domain.service.ConnectionRegistry;
import com.sparrow.messaging.domain.service.MultiDeviceConnectionRegistry;
import com.sparrow.messaging.domain.service.pub.sub.typed.TypedPublishStrategy;
import com.sparrow.messaging.protocol.enums.ChatType;
import com.sparrow.messaging.domain.service.pub.sub.typed.mapping.TypedPubSubMappingContainer;
import com.sparrow.messaging.protocol.EventType;
import com.sparrow.messaging.protocol.Protocol;
import com.sparrow.messaging.protocol.api.TypedSubscriber;
import com.sparrow.messaging.protocol.enums.DefaultEventType;
import com.sparrow.protocol.enums.DeviceType;
import com.sparrow.utility.ClassUtility;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class WebSocketGatewayTest {
    @Test
    void unsupportedEventClosesTheConnectionWithoutNullPointer() throws Exception {
        var directory = new MultiDeviceConnectionRegistry();
        var channel = new EmbeddedChannel(new WebSocketGateway());
        try (var bindings = new Bindings()) {
            bindings.bind(ConfigReader.class, new SparrowConfigReader());
            bindings.bind(ConnectionRegistry.class, directory);
            bindings.bind(TypedPubSubMappingContainer.class, new TypedPubSubMappingContainer());
            var user = new DefaultLoginUser();
            user.setUserId(1L);user.setCategory(1);user.setDeviceType(DeviceType.PC);
            directory.register(new NettyWebsocketConnection(channel, user));
            assertDoesNotThrow(() -> channel.writeInbound(new BinaryWebSocketFrame(
                    Unpooled.wrappedBuffer(new byte[]{40}))));
            assertFalse(channel.isActive());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void replacedPhysicalConnectionCannotDispatchOrRefreshTheNewConnection() throws Exception {
        var directory = new MultiDeviceConnectionRegistry();
        var mappings = new TypedPubSubMappingContainer();
        var delivered = new AtomicInteger();
        mappings.put(new TypedSubscriber<Protocol>() {
            public EventType getEventType() { return DefaultEventType.PING; }
            public void subscribe(Protocol event) { delivered.incrementAndGet(); }
        });
        var staleChannel = new EmbeddedChannel(new WebSocketGateway());
        var newChannel = new EmbeddedChannel();
        try (var bindings = new Bindings()) {
            bindings.bind(ConfigReader.class, new SparrowConfigReader());
            bindings.bind(ConnectionRegistry.class, directory);
            bindings.bind(TypedPubSubMappingContainer.class, mappings);
            var user = new DefaultLoginUser();
            user.setUserId(1L);
            user.setCategory(1);
            user.setDeviceType(DeviceType.PC);
            var stale = new NettyWebsocketConnection(staleChannel, user);
            var replacement = new NettyWebsocketConnection(newChannel, user);
            directory.register(stale);
            directory.register(replacement);
            newChannel.attr(NettyWebsocketConnection.LAST_ACCESS_TIME).set(1L);
            staleChannel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{30})));
            assertFalse(staleChannel.isActive());
            assertEquals(0, delivered.get());
            assertEquals(1L, replacement.getLastAccessTime());
            assertSame(replacement, directory.findConnection(user));
            assertTrue(newChannel.isActive());
        } finally {
            staleChannel.finishAndReleaseAll();
            newChannel.finishAndReleaseAll();
        }
    }

    @Test
    void malformedFrameAndBusinessFailureCloseWithoutAcknowledging() throws Exception {
        for (boolean malformed : new boolean[]{true, false}) {
            var directory = new MultiDeviceConnectionRegistry();
            var channel = new EmbeddedChannel(new WebSocketGateway());
            try (var bindings = new Bindings()) {
                bindings.bind(ConfigReader.class, new SparrowConfigReader());
                bindings.bind(ConnectionRegistry.class, directory);
                bindings.bind(TypedPubSubMappingContainer.class, new TypedPubSubMappingContainer());
                bindings.bind(TypedPublishStrategy.class, new TypedPublishStrategy() {
                    public void publish(com.sparrow.messaging.protocol.TypedEvent event) {
                        throw new IllegalStateException("business failed");
                    }
                });
                var user = new DefaultLoginUser();user.setUserId(1L);user.setCategory(1);user.setDeviceType(DeviceType.PC);
                directory.register(new NettyWebsocketConnection(channel, user));
                var message = new Protocol();message.setEventType(DefaultEventType.IM);
                message.setDevice(DeviceType.PC);message.setChatType(ChatType.ONE_2_ONE);message.setClientSendTime(1L);
                message.setSender(com.sparrow.messaging.protocol.ChatUser.longUserId(1L, 1));
                message.setReceiver(com.sparrow.messaging.protocol.ChatUser.longUserId(2L, 1));
                var frame = malformed ? Unpooled.wrappedBuffer(new byte[]{0}) : Unpooled.wrappedBuffer(message.encode());
                assertDoesNotThrow(() -> channel.writeInbound(new BinaryWebSocketFrame(frame)));
                assertFalse(channel.isActive());
                assertNull(channel.readOutbound(), "Rejected messages must not receive a success ACK");
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    private static final class Bindings implements AutoCloseable {
        @SuppressWarnings("unchecked")
        private final FactoryBean<Object> objects = ApplicationContext.getContainer().getSingletonRegister();
        private final Map<String, Object> previous = new LinkedHashMap<>();
        void bind(Class<?> type, Object value) {
            String name = ClassUtility.getBeanNameByClass(type);
            previous.put(name, objects.getObject(name));
            objects.pubObject(name, value);
        }
        public void close() {
            previous.forEach((name, value) -> {
                if (value == null) {
                    objects.removeObject(name);
                } else {
                    objects.pubObject(name, value);
                }
            });
        }
    }
}
