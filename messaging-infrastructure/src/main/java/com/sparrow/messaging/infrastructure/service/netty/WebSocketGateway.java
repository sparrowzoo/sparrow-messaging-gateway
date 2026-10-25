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
import com.sparrow.messaging.domain.service.PublishStrategy;
import com.sparrow.messaging.domain.service.RoutableTypedHandlerMappingContainer;
import com.sparrow.messaging.protocol.ChatUser;
import com.sparrow.messaging.protocol.EventType;
import com.sparrow.messaging.protocol.Protocol;
import com.sparrow.protocol.LoginUser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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
            PublishStrategy publishStrategy = ApplicationContext.getContainer().getBean(PublishStrategy.class);
            RoutableTypedHandlerMappingContainer eventContainer = ApplicationContext.getContainer().getBean(RoutableTypedHandlerMappingContainer.class);
            ConnectionRegistry connectionRegistry = ApplicationContext.getContainer().getBean(ConnectionRegistry.class);
            LoginUser loginUser = ctx.channel().attr(NettyWebsocketConnection.LOGIN_USER).get();
            ClientConnection clientConnection = connectionRegistry.findConnection(loginUser);
            clientConnection.refreshAccessTime();
            if (frame instanceof BinaryWebSocketFrame msg) {
                ByteBuf content = msg.content();
                Protocol protocol = new Protocol(content.nioBuffer());
                protocol.setSender(ChatUser.longUserId(loginUser.getUserId(), loginUser.getCategory()));
                EventType eventType = protocol.getEventType();
                if (eventType.publishable()) {
                    publishStrategy.publish(protocol);
                }
                eventContainer.getEventHandler(protocol).handle(protocol);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
