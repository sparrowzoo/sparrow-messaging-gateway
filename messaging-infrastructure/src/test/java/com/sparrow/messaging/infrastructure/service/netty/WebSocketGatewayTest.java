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
import com.sparrow.messaging.protocol.ProtocolType;
import com.sparrow.messaging.protocol.Protocol;
import com.sparrow.messaging.protocol.api.TypedSubscriber;
import com.sparrow.messaging.protocol.enums.DefaultProtocolType;
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
    void unsupportedEventReportsErrorAndKeepsConnection() throws Exception {
        var directory = new MultiDeviceConnectionRegistry();
        var channel = new EmbeddedChannel(new WebSocketGateway());
        try (var bindings = new Bindings()) {
            bindings.bind(ConfigReader.class, new SparrowConfigReader());
        bindings.bind(com.sparrow.messaging.protocol.api.SeedService.class,
                new com.sparrow.messaging.domain.service.LocalMemorySeedService());
            bindings.bind(ConnectionRegistry.class, directory);
            bindings.bind(TypedPubSubMappingContainer.class, new TypedPubSubMappingContainer());
            var user = new DefaultLoginUser();
            user.setUserId(1L);user.setCategory(1);user.setDeviceType(DeviceType.PC);
            directory.register(new NettyWebsocketConnection(channel, user));
            assertDoesNotThrow(() -> channel.writeInbound(new BinaryWebSocketFrame(
                    Unpooled.wrappedBuffer(new byte[]{70}))));
            channel.runPendingTasks();
            assertTrue(channel.isActive());
            assertErrorResponse(channel);
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
            public ProtocolType getProtocolType() { return DefaultProtocolType.FETCH_USER_STATUS; }
            public void subscribe(Protocol event) { delivered.incrementAndGet(); }
        });
        var staleChannel = new EmbeddedChannel(new WebSocketGateway());
        var newChannel = new EmbeddedChannel();
        try (var bindings = new Bindings()) {
            bindings.bind(ConfigReader.class, new SparrowConfigReader());
        bindings.bind(com.sparrow.messaging.protocol.api.SeedService.class,
                new com.sparrow.messaging.domain.service.LocalMemorySeedService());
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
            staleChannel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{45})));
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
    void malformedFrameClosesWithoutAcknowledging() throws Exception {
        var directory = new MultiDeviceConnectionRegistry();
        var channel = new EmbeddedChannel(new WebSocketGateway());
        try (var bindings = new Bindings()) {
            bindConnection(bindings, directory, channel);
            bindings.bind(TypedPubSubMappingContainer.class, new TypedPubSubMappingContainer());
            channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{0})));
            assertFalse(channel.isActive());
            assertNull(channel.readOutbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void businessFailureReportsErrorAndAllowsTheNextMessage() throws Exception {
        for (Throwable failure : new Throwable[]{new IllegalStateException("business failed"),
                new IllegalArgumentException("business argument invalid"), new Exception("publish failed")}) {
            var directory = new MultiDeviceConnectionRegistry();
            var channel = new EmbeddedChannel(new WebSocketGateway());
            var receipts = new AtomicInteger();
            var mappings = new TypedPubSubMappingContainer();
            mappings.put(new TypedSubscriber<Protocol>() {
                public ProtocolType getProtocolType() { return DefaultProtocolType.IM; }
                public void subscribe(Protocol event) { receipts.incrementAndGet(); }
            });
            mappings.put(new TypedSubscriber<Protocol>() {
                public ProtocolType getProtocolType() { return DefaultProtocolType.FETCH_USER_STATUS; }
                public void subscribe(Protocol event) { receipts.incrementAndGet(); }
            });
            try (var bindings = new Bindings()) {
                bindConnection(bindings, directory, channel);
                bindings.bind(TypedPubSubMappingContainer.class, mappings);
                bindings.bind(TypedPublishStrategy.class, new TypedPublishStrategy() {
                    public void publish(com.sparrow.messaging.protocol.TypedEvent event) throws Throwable {
                        throw failure;
                    }
                });
                channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(message().encode())));
                channel.runPendingTasks();
                assertTrue(channel.isActive(), "Business failures must not disconnect the client");
                assertErrorResponse(channel);
                channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{45})));
                channel.runPendingTasks();
                assertTrue(channel.isActive());
                assertEquals(2, receipts.get(), "Receipt processing and subsequent messages remain available");
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    void queuedReplyFailureReportsErrorAndKeepsConnection() throws Exception {
        var directory = new MultiDeviceConnectionRegistry();
        var channel = new EmbeddedChannel(new WebSocketGateway());
        var mappings = new TypedPubSubMappingContainer();
        mappings.put(new TypedSubscriber<Protocol>() {
            public ProtocolType getProtocolType() { return DefaultProtocolType.FETCH_USER_STATUS; }
            public void subscribe(Protocol event) { throw new IllegalStateException("reply failed"); }
        });
        try (var bindings = new Bindings()) {
            bindConnection(bindings, directory, channel);
            bindings.bind(TypedPubSubMappingContainer.class, mappings);
            channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{45})));
            channel.runPendingTasks();
            assertTrue(channel.isActive());
            assertErrorResponse(channel);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void fatalErrorInQueuedReplyClosesConnection() throws Exception {
        var directory = new MultiDeviceConnectionRegistry();
        var channel = new EmbeddedChannel(new WebSocketGateway());
        var mappings = new TypedPubSubMappingContainer();
        var fatal = new LinkageError("fatal reply failure");
        mappings.put(new TypedSubscriber<Protocol>() {
            public ProtocolType getProtocolType() { return DefaultProtocolType.FETCH_USER_STATUS; }
            public void subscribe(Protocol event) { throw fatal; }
        });
        try (var bindings = new Bindings()) {
            bindConnection(bindings, directory, channel);
            bindings.bind(TypedPubSubMappingContainer.class, mappings);
            assertSame(fatal, assertThrows(LinkageError.class, () -> channel.writeInbound(
                    new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{45})))));
            channel.runPendingTasks();
            assertFalse(channel.isActive());
            assertNull(channel.readOutbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void queuedReceiptUsesTheReceivedMessageWhenPublisherChangesItsCopy() throws Exception {
        var directory = new MultiDeviceConnectionRegistry();
        var channel = new EmbeddedChannel(new WebSocketGateway());
        var received = new java.util.concurrent.atomic.AtomicReference<Protocol>();
        var mappings = new TypedPubSubMappingContainer();
        mappings.put(new TypedSubscriber<Protocol>() {
            public ProtocolType getProtocolType() { return DefaultProtocolType.IM; }
            public void subscribe(Protocol event) { received.set(event); }
        });
        try (var bindings = new Bindings()) {
            bindConnection(bindings, directory, channel);
            bindings.bind(TypedPubSubMappingContainer.class, mappings);
            bindings.bind(TypedPublishStrategy.class, new TypedPublishStrategy() {
                public void publish(com.sparrow.messaging.protocol.TypedEvent event) {
                    var changed = (Protocol) event;
                    changed.setProtocolType(DefaultProtocolType.OFFLINE);
                    changed.setClientSendTime(999L);
                }
            });
            channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(message().encode())));
            channel.runPendingTasks();
            assertNotNull(received.get());
            assertEquals(DefaultProtocolType.IM, received.get().getProtocolType());
            assertEquals(1L, received.get().getClientSendTime());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void replacedConnectionDoesNotRunAQueuedReceipt() throws Exception {
        var directory = new MultiDeviceConnectionRegistry();
        var channel = new EmbeddedChannel(new WebSocketGateway());
        var replacementChannel = new EmbeddedChannel();
        var replies = new AtomicInteger();
        var mappings = new TypedPubSubMappingContainer();
        mappings.put(new TypedSubscriber<Protocol>() {
            public ProtocolType getProtocolType() { return DefaultProtocolType.IM; }
            public void subscribe(Protocol event) { replies.incrementAndGet(); }
        });
        try (var bindings = new Bindings()) {
            bindConnection(bindings, directory, channel);
            bindings.bind(TypedPubSubMappingContainer.class, mappings);
            bindings.bind(TypedPublishStrategy.class, new TypedPublishStrategy() {
                public void publish(com.sparrow.messaging.protocol.TypedEvent event) {
                    var user = new DefaultLoginUser();
                    user.setUserId(1L);user.setCategory(1);user.setDeviceType(DeviceType.PC);
                    directory.register(new NettyWebsocketConnection(replacementChannel, user));
                }
            });
            channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(message().encode())));
            channel.runPendingTasks();
            assertEquals(0, replies.get(), "Queued replies must not follow a replaced connection");
            assertFalse(channel.isActive());
            assertTrue(replacementChannel.isActive());
        } finally {
            channel.finishAndReleaseAll();
            replacementChannel.finishAndReleaseAll();
        }
    }

    @Test
    void statusQueryDoesNotAllocateBusinessSequence() throws Exception {
        var directory = new MultiDeviceConnectionRegistry();
        var channel = new EmbeddedChannel(new WebSocketGateway());
        var calls = new AtomicInteger();
        var mappings = new TypedPubSubMappingContainer();
        mappings.put(new TypedSubscriber<Protocol>() {
            public ProtocolType getProtocolType() { return DefaultProtocolType.FETCH_USER_STATUS; }
            public void subscribe(Protocol event) {
                assertNull(event.getChatSession());
                assertNull(event.getSeqNo());
                calls.incrementAndGet();
            }
        });
        try (var bindings = new Bindings()) {
            bindConnection(bindings, directory, channel);
            bindings.bind(com.sparrow.messaging.protocol.api.SeedService.class,
                    new com.sparrow.messaging.protocol.api.SeedService() {
                        public Long nextId(String key) {
                            fail("A status query must not allocate a business sequence");
                            return null;
                        }
                    });
            bindings.bind(TypedPubSubMappingContainer.class, mappings);
            channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{45})));
            channel.runPendingTasks();
            assertEquals(1, calls.get());
            assertTrue(channel.isActive());
            assertNull(channel.readOutbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static void bindConnection(Bindings bindings, MultiDeviceConnectionRegistry directory,
            EmbeddedChannel channel) throws Exception {
        bindings.bind(ConfigReader.class, new SparrowConfigReader());
        bindings.bind(com.sparrow.messaging.protocol.api.SeedService.class,
                new com.sparrow.messaging.domain.service.LocalMemorySeedService());
        bindings.bind(ConnectionRegistry.class, directory);
        var user = new DefaultLoginUser();
        user.setUserId(1L);user.setCategory(1);user.setDeviceType(DeviceType.PC);
        directory.register(new NettyWebsocketConnection(channel, user));
    }

    private static Protocol message() {
        var message = new Protocol();message.setProtocolType(DefaultProtocolType.IM);
        message.setDevice(DeviceType.PC);message.setChatType(ChatType.ONE_2_ONE);message.setClientSendTime(1L);
        message.setSender(com.sparrow.messaging.protocol.ChatUser.longUserId(1L, 1));
        message.setReceiver(com.sparrow.messaging.protocol.ChatUser.longUserId(2L, 1));
        return message;
    }

    private static void assertErrorResponse(EmbeddedChannel channel) {
        BinaryWebSocketFrame response = channel.readOutbound();
        assertNotNull(response, "A failed message must receive an error response");
        try {
            assertEquals(DefaultProtocolType.ERROR, new Protocol(response.content().nioBuffer()).getProtocolType());
        } finally {
            response.release();
        }
    }

    private static final class Bindings implements AutoCloseable {
        @SuppressWarnings("unchecked")
        private final FactoryBean<Object> objects = ApplicationContext.getContainer().getSingletonRegister();
        private final Map<String, Object> previous = new LinkedHashMap<>();
        void bind(Class<?> type, Object value) {
            String name = ClassUtility.getBeanNameByClass(type);
            if (!previous.containsKey(name)) {
                previous.put(name, objects.getObject(name));
            }
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
