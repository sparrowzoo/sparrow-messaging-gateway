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
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class NettyWebsocketConnectionTest {
    @Test
    void newConnectionAlreadyHasAnAccessTimeAndDuration() {
        var channel = new EmbeddedChannel();
        try {
            var connection = new NettyWebsocketConnection(channel, new DefaultLoginUser());
            assertNotNull(connection.getLastAccessTime());
            assertEquals(0L, connection.getDuration());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void unwritableChannelRejectsWithoutQueuingAFrame() {
        var channel = new EmbeddedChannel();
        try {
            var connection = new NettyWebsocketConnection(channel, new DefaultLoginUser());
            channel.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
            var failure = assertThrows(CompletionException.class, () ->
                    connection.sendMessage(ByteBuffer.wrap(new byte[]{40})).toCompletableFuture().join());
            assertInstanceOf(RejectedExecutionException.class, failure.getCause());
            assertNull(channel.readOutbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void sendPreservesCallerPositionAndCloseRejectsFurtherSends() {
        var channel = new EmbeddedChannel();
        try {
            var connection = new NettyWebsocketConnection(channel, new DefaultLoginUser());
            var bytes = ByteBuffer.wrap(new byte[]{0, 40});
            bytes.position(1);
            connection.sendMessage(bytes).toCompletableFuture().join();
            BinaryWebSocketFrame frame = channel.readOutbound();
            assertEquals(1, frame.content().readableBytes());
            assertEquals(40, frame.content().readUnsignedByte());
            frame.release();
            assertEquals(1, bytes.position());
            connection.closeConnection();
            connection.closeConnection();
            assertThrows(CompletionException.class, () ->
                    connection.sendMessage(bytes).toCompletableFuture().join());
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
