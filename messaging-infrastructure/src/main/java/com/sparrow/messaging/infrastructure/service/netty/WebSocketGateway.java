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

import com.sparrow.core.spi.ApplicationContext;
import com.sparrow.messaging.domain.service.ClientConnection;
import com.sparrow.messaging.domain.service.ConnectionRegistry;
import com.sparrow.messaging.domain.service.pub.sub.typed.TypedPublishStrategy;
import com.sparrow.messaging.domain.service.pub.sub.typed.mapping.TypedPubSubMappingContainer;
import com.sparrow.messaging.protocol.ChatUser;
import com.sparrow.messaging.protocol.EventType;
import com.sparrow.messaging.protocol.Protocol;
import com.sparrow.protocol.LoginUser;
import io.netty.buffer.ByteBuf;
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
            ClientConnection clientConnection = connectionRegistry.findConnection(loginUser);
            if (clientConnection == null || clientConnection != ctx.channel().attr(NettyWebsocketConnection.CONNECTION).get()) {
                ctx.close();
                return;
            }
            clientConnection.refreshAccessTime();
            if (frame instanceof BinaryWebSocketFrame msg) {
                ByteBuf content = msg.content();
                Protocol protocol = new Protocol(content.nioBuffer());
                protocol.setSender(ChatUser.longUserId(loginUser.getUserId(), loginUser.getCategory()));
                protocol.setDevice(loginUser.getDeviceType());
                EventType eventType = protocol.getEventType();
                if (eventType.publishable()) {
                    TypedPublishStrategy publishStrategy = ApplicationContext.getContainer().getBean(TypedPublishStrategy.class);
                    publishStrategy.publish(protocol);
                }
                var eventHandler = eventContainer.getEventHandler(protocol);
                if (eventHandler == null) {
                    log.warn("Unsupported event type: {}", eventType);
                    ctx.close();
                    return;
                }
                eventHandler.subscribe(protocol);
            }
        } catch (Throwable e) {
            ctx.close();
            if (e instanceof Error error) {
                throw error;
            }
            log.warn("WebSocket message rejected or business handling failed", e);
        }
    }
}
