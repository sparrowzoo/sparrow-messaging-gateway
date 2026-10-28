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

/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.sparrow.messaging.infrastructure.service.netty;

import com.sparrow.context.SessionContext;
import com.sparrow.core.spi.ApplicationContext;
import com.sparrow.messaging.domain.service.ClientConnection;
import com.sparrow.messaging.domain.service.ConnectionRegistry;
import com.sparrow.messaging.domain.service.pub.sub.typed.TypedPublishStrategy;
import com.sparrow.messaging.domain.service.pub.sub.typed.mapping.TypedPubSubMappingContainer;
import com.sparrow.messaging.protocol.ChatUser;
import com.sparrow.messaging.protocol.Protocol;
import com.sparrow.messaging.protocol.ProtocolType;
import com.sparrow.messaging.protocol.enums.DefaultProtocolType;
import com.sparrow.protocol.LoginUser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ChannelHandler.Sharable
public class WebSocketGateway extends SimpleChannelInboundHandler<WebSocketFrame> {

    /**
     * 一定要重写channelRead0方法，否则会报错,内存泄漏问题交由netty处理
     *
     * @param ctx   the {@link ChannelHandlerContext} which this {@link SimpleChannelInboundHandler}
     *              belongs to
     * @param frame the message to handle
     * @throws Exception
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) throws Exception {
        try {
            TypedPubSubMappingContainer eventContainer = ApplicationContext.getContainer().getBean(TypedPubSubMappingContainer.class);
            ConnectionRegistry connectionRegistry = ApplicationContext.getContainer().getBean(ConnectionRegistry.class);
            LoginUser loginUser = ctx.channel().attr(NettyWebsocketConnection.LOGIN_USER).get();
            if (loginUser == null) {
                ctx.close();
                return;
            }
            SessionContext.bindLoginUser(loginUser);
            ClientConnection clientConnection = connectionRegistry.findConnection(loginUser);
            if (clientConnection == null || clientConnection != ctx.channel().attr(NettyWebsocketConnection.CONNECTION).get()) {
                ctx.close();
                return;
            }
            clientConnection.refreshAccessTime();
            if (frame instanceof BinaryWebSocketFrame msg) {
                ByteBuf content = msg.content();
                Protocol protocol;
                try {
                    protocol = new Protocol(content.nioBuffer());
                } catch (IllegalArgumentException e) {
                    log.warn("Malformed WebSocket protocol; closing connection", e);
                    ctx.close();
                    return;
                }
                protocol.setSender(ChatUser.longUserId(loginUser.getUserId(), loginUser.getCategory()));
                protocol.setDevice(loginUser.getDeviceType());
                protocol.setLoginUser(loginUser);
                ProtocolType protocolType = protocol.getProtocolType();
                if (protocolType.localImmediately()) {
                    // Publisher 可以修改消息；接收回执使用入站时的字段快照。
                    Protocol localProtocol = protocol.clone();
                    ctx.executor().execute(() -> {
                        if (!ctx.channel().isActive()) {
                            return;
                        }
                        try {
                            // 排队期间同设备可能重连，不把旧消息的回执送到新连接。
                            if (connectionRegistry.findConnection(loginUser) != clientConnection) {
                                ctx.close();
                                return;
                            }
                            var eventHandler = eventContainer.getImmediately(localProtocol);
                            if (eventHandler == null) {
                                log.warn("Unsupported protocol type: {}", protocolType);
                                sendErrorResponse(ctx);
                                return;
                            }
                            eventHandler.subscribe(localProtocol);
                        } catch (Throwable e) {
                            handleMessageFailure(ctx, e);
                        }
                    });
                }
                if (protocolType.needBusiness()) {
                    TypedPublishStrategy publishStrategy = ApplicationContext.getContainer().getBean(TypedPublishStrategy.class);
                    publishStrategy.publish(protocol);
                }
            }
        } catch (Throwable e) {
            handleMessageFailure(ctx, e);
        } finally {
            SessionContext.clearToken();
        }
    }

    private void handleMessageFailure(ChannelHandlerContext ctx, Throwable failure) {
        if (failure instanceof Error error) {
            ctx.close();
            throw error;
        }
        log.warn("WebSocket message handling failed; keeping connection open", failure);
        sendErrorResponse(ctx);
    }

    private void sendErrorResponse(ChannelHandlerContext ctx) {
        if (!ctx.channel().isActive()) {
            return;
        }
        Protocol response = new Protocol();
        response.setProtocolType(DefaultProtocolType.ERROR);
        ctx.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(response.encode())))
                .addListener(future -> {
                    if (!future.isSuccess()) {
                        log.warn("Failed to send WebSocket error response", future.cause());
                    }
                });
    }
}
