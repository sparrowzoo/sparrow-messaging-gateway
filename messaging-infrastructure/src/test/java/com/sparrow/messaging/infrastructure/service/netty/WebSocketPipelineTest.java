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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class WebSocketPipelineTest {
    private static final int LIMIT = 655360;

    @Test
    void handshakeDoesNotNegotiateCompression() {
        try (var server = new Server()) {
            assertFalse(server.response.toLowerCase().contains("sec-websocket-extensions"));
            assertTrue(server.response.contains("101 Switching Protocols"));
        }
    }

    @Test
    void compressedFrameIsRejectedEvenWhenCompressedPayloadIsSmall() {
        try (var server = new Server()) {
            byte[] raw = new byte[2 * 1024 * 1024];
            java.util.Arrays.fill(raw, (byte) 'A');
            var compressor = new java.util.zip.Deflater(9, true);
            byte[] compressed = new byte[4096];
            int size;
            try {
                compressor.setInput(raw);
                size = compressor.deflate(compressed, 0, compressed.length, java.util.zip.Deflater.SYNC_FLUSH);
            } finally {
                compressor.end();
            }
            assertFalse(server.receive(new BinaryWebSocketFrame(true, 4,
                    Unpooled.wrappedBuffer(java.util.Arrays.copyOf(compressed, size - 4)))));
            assertFalse(server.channel.isActive());
        }
    }

    @Test
    void acceptsExactlyTheLimitAndRejectsAnOversizedSingleFrame() {
        try (var server = new Server()) {
            assertTrue(server.receive(new BinaryWebSocketFrame(Unpooled.buffer(LIMIT).writeZero(LIMIT))));
            WebSocketFrame frame = server.channel.readInbound();
            assertEquals(LIMIT, frame.content().readableBytes());
            frame.release();
            assertFalse(server.receive(new BinaryWebSocketFrame(Unpooled.buffer(LIMIT + 1).writeZero(LIMIT + 1))));
            assertNull(server.channel.readInbound());
            assertFalse(server.channel.isActive());
        }
    }

    @Test
    void fragmentedMessageCannotExceedTotalLimit() {
        try (var server = new Server()) {
            assertFalse(server.receive(new BinaryWebSocketFrame(false, 0,
                    Unpooled.buffer(LIMIT).writeZero(LIMIT))));
            assertFalse(server.receive(new ContinuationWebSocketFrame(true, 0, Unpooled.wrappedBuffer(new byte[]{1}))));
            assertFalse(server.channel.isActive());
            assertNull(server.channel.readInbound());
        }
    }

    @Test
    void heartbeatDoesNotWriteControlFramesBeforeHandshakeCompletes() {
        EmbeddedChannel channel = new EmbeddedChannel(new WebSocketHeartbeatHandler());
        try {
            channel.advanceTimeBy(11, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertNull(channel.readOutbound());
            assertTrue(channel.isActive());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void readIdleSendsNativePingAndClosesAfterThirtySecondsWithoutInput() {
        try (Server server = new Server()) {
            for (int i = 0; i < 2; i++) {
                server.advance(10);
                assertTrue(server.channel.isActive());
                WebSocketFrame frame = server.readFrame();
                assertInstanceOf(PingWebSocketFrame.class, frame);
                frame.release();
            }
            server.advance(11);
            assertFalse(server.channel.isActive());
        }
    }

    @Test
    void nativePongKeepsConnectionAliveAndNeverReachesBusinessHandlers() {
        try (Server server = new Server()) {
            for (int i = 0; i < 4; i++) {
                server.advance(11);
                assertTrue(server.channel.isActive());
                WebSocketFrame ping = server.readFrame();
                assertInstanceOf(PingWebSocketFrame.class, ping);
                ping.release();
                assertFalse(server.receive(new PongWebSocketFrame()));
                assertNull(server.channel.readInbound());
                assertTrue(server.channel.isActive());
            }
        }
    }

    @Test
    void outgoingBusinessMessagesDoNotSuppressProbesOrExtendReadTimeout() {
        try (Server server = new Server()) {
            for (int i = 0; i < 2; i++) {
                server.channel.writeAndFlush(new TextWebSocketFrame("stream chunk"));
                ReferenceCountUtil.release(server.readFrame());
                server.advance(10);
                WebSocketFrame ping = server.readFrame();
                assertInstanceOf(PingWebSocketFrame.class, ping);
                ping.release();
            }
            server.channel.writeAndFlush(new TextWebSocketFrame("stream chunk"));
            ReferenceCountUtil.release(server.readFrame());
            server.advance(11);
            assertFalse(server.channel.isActive());
        }
    }

    @Test
    void nativeClientPingIsAnsweredByNettyWithoutEnteringBusinessHandlers() {
        try (Server server = new Server()) {
            assertFalse(server.receive(new PingWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{1, 2, 3}))));
            WebSocketFrame pong = server.readFrame();
            try {
                assertInstanceOf(PongWebSocketFrame.class, pong);
                assertEquals(3, pong.content().readableBytes());
                assertEquals(1, pong.content().readByte());
                assertEquals(2, pong.content().readByte());
                assertEquals(3, pong.content().readByte());
            } finally {
                pong.release();
            }
            assertNull(server.channel.readInbound());
        }
    }

    @Test
    void businessInputAlsoResetsReadIdle() {
        try (Server server = new Server()) {
            for (int i = 0; i < 5; i++) {
                server.advance(8);
                assertTrue(server.receive(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[]{45}))));
                ReferenceCountUtil.release(server.channel.readInbound());
                assertNull(server.channel.readOutbound());
                assertTrue(server.channel.isActive());
            }
        }
    }

    private static final class Server implements AutoCloseable {
        final EmbeddedChannel channel = new EmbeddedChannel();
        final String response;

        Server() {
            SocketChannel socket = (SocketChannel) Proxy.newProxyInstance(SocketChannel.class.getClassLoader(),
                    new Class<?>[]{SocketChannel.class}, (proxy, method, args) -> {
                        if (method.getName().equals("pipeline")) {
                            return channel.pipeline();
                        }
                        throw new UnsupportedOperationException(method.getName());
                    });
            new WebSocketServerInitializer(null).initChannel(socket);
            channel.pipeline().remove(WebSocketGateway.class);
            // Test the configured HTTP/WebSocket pipeline, without running application authentication.
            String protocol = channel.pipeline().context(WebSocketServerProtocolSupportHandshake.class).name();
            channel.pipeline().addBefore(protocol, "authentication-boundary", new ChannelInboundHandlerAdapter() {
                @Override
                public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
                    if (!(event instanceof WebSocketServerProtocolHandler.HandshakeComplete)) {
                        ctx.fireUserEventTriggered(event);
                    }
                }
            });
            channel.writeInbound(Unpooled.copiedBuffer("GET /websocket HTTP/1.1\r\n"
                    + "Host: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                    + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
                    + "Sec-WebSocket-Extensions: permessage-deflate\r\n\r\n", CharsetUtil.US_ASCII));
            StringBuilder text = new StringBuilder();
            Object output;
            while ((output = channel.readOutbound()) != null) {
                if (output instanceof ByteBuf bytes) {
                    text.append(bytes.toString(CharsetUtil.US_ASCII));
                }
                ReferenceCountUtil.release(output);
            }
            response = text.toString();
        }

        void advance(int seconds) {
            channel.advanceTimeBy(seconds, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
        }

        WebSocketFrame readFrame() {
            EmbeddedChannel decoder = new EmbeddedChannel(new WebSocket13FrameDecoder(false, false, LIMIT));
            try {
                Object bytes;
                while ((bytes = channel.readOutbound()) != null) {
                    decoder.writeInbound(bytes);
                }
                return decoder.readInbound();
            } finally {
                decoder.finishAndReleaseAll();
            }
        }

        boolean receive(WebSocketFrame frame) {
            var client = new EmbeddedChannel(new WebSocket13FrameEncoder(true));
            try {
                client.writeOutbound(frame);
                boolean received = false;
                ByteBuf bytes;
                while ((bytes = client.readOutbound()) != null) {
                    received |= channel.writeInbound(bytes);
                }
                return received;
            } finally {
                client.finishAndReleaseAll();
            }
        }

        @Override
        public void close() {
            channel.finishAndReleaseAll();
        }
    }
}
