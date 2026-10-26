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

import com.sparrow.messaging.domain.service.ClientConnection;
import com.sparrow.protocol.LoginUser;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class NettyWebsocketConnection implements ClientConnection {
    private static final int CLOSE_TIMEOUT_SECONDS = 5;
    public static final AttributeKey<LoginUser> LOGIN_USER = AttributeKey.newInstance("loginUser");
    public static final AttributeKey<ClientConnection> CONNECTION = AttributeKey.valueOf("messaging.connection");
    public static final AttributeKey<Long> CREATE_TIME = AttributeKey.newInstance("createTime");

    public static final AttributeKey<Long> LAST_ACCESS_TIME = AttributeKey.newInstance("lastActiveTime");
    private final Channel channel;
    private final LoginUser loginUser;
    private final AtomicBoolean closing = new AtomicBoolean();

    public NettyWebsocketConnection(Channel channel, LoginUser loginUser) {
        this.channel = Objects.requireNonNull(channel, "channel");
        channel.attr(LOGIN_USER).set(loginUser);
        channel.attr(CONNECTION).set(this);
        long now = System.currentTimeMillis();
        channel.attr(CREATE_TIME).set(now);
        channel.attr(LAST_ACCESS_TIME).set(now);
        this.loginUser = loginUser;
    }

    @Override
    public LoginUser loginUser() {
        return loginUser;
    }

    @Override
    public CompletionStage<Void> sendMessage(ByteBuffer encodedFrame) {
        this.channel.attr(LAST_ACCESS_TIME).set(System.currentTimeMillis());
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (encodedFrame == null) {
            result.completeExceptionally(new NullPointerException("encodedFrame"));
            return result;
        }
        BinaryWebSocketFrame frame = null;
        try {
            if (closing.get() || !channel.isActive()) {
                throw new ClosedChannelException();
            }
            if (!channel.isWritable()) {
                throw new RejectedExecutionException("Channel is not writable");
            }

            // 独立位置，共享不可变内容；同一报文发往多个连接时无需重复编码。
            // 调用方必须保持底层数据不变，直到所有连接的发送完成。
            frame = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(encodedFrame.duplicate()));
            ChannelFuture write = channel.writeAndFlush(frame);
            // writeAndFlush 接管帧及其 ByteBuf，异步成功或失败均由 Netty 释放。
            frame = null;
            write.addListener(future -> {
                if (future.isSuccess()) {
                    result.complete(null);
                } else if (future.isCancelled()) {
                    result.cancel(false);
                } else {
                    result.completeExceptionally(future.cause());
                }
            });
        } catch (Exception exception) {
            ReferenceCountUtil.safeRelease(frame);
            result.completeExceptionally(exception);
        }
        return result;
    }

    @Override
    public void closeConnection() {
        if (!closing.compareAndSet(false, true)) {
            return;
        }
        if (!channel.isActive()) {
            channel.close();
            return;
        }

        CloseWebSocketFrame frame = null;
        try {
            // 关闭帧也可能被慢连接堵住，超时后直接关闭，避免一直占用资源。
            ScheduledFuture<?> timeout = channel.eventLoop().schedule(() -> {
                channel.close();
            }, CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            channel.closeFuture().addListener(future -> timeout.cancel(false));

            frame = new CloseWebSocketFrame(WebSocketCloseStatus.NORMAL_CLOSURE);
            ChannelFuture write = channel.writeAndFlush(frame);
            frame = null;
            // 写完关闭帧或写失败后关闭，不阻塞调用线程。
            write.addListener(ChannelFutureListener.CLOSE);
        } catch (Exception exception) {
            ReferenceCountUtil.safeRelease(frame);
            channel.close();
        }
    }

    @Override
    public Long getLastAccessTime() {
        return this.channel.attr(LAST_ACCESS_TIME).get();
    }

    @Override
    public void refreshAccessTime() {
        this.channel.attr(LAST_ACCESS_TIME).set(System.currentTimeMillis());
    }

    @Override
    public Long getCreateTime() {
        return this.channel.attr(CREATE_TIME).get();
    }

    @Override
    public Long getDuration() {
        return this.getLastAccessTime() - this.getCreateTime();
    }
}
