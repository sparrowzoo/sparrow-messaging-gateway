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

package com.sparrow.messaging.messaging.example.typed.event;

import com.sparrow.authenticator.DefaultLoginUser;
import com.sparrow.messaging.domain.service.ConnectionRegistry;
import com.sparrow.messaging.domain.service.pub.sub.typed.mapping.TypedPubSubMappingContainer;
import com.sparrow.messaging.infrastructure.service.netty.NettyWebsocketConnection;
import com.sparrow.messaging.protocol.ChatUser;
import com.sparrow.messaging.protocol.Protocol;
import com.sparrow.protocol.enums.DefaultUserCategory;
import com.sparrow.protocol.enums.DeviceType;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.springframework.context.ApplicationContext;

/**
 * 用内存通道演示事件收发，复用项目的连接、协议和事件订阅实现。
 */
public class EventExampleClient implements AutoCloseable {
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final DefaultLoginUser loginUser = new DefaultLoginUser();
    private final ConnectionRegistry connections;
    private final TypedPubSubMappingContainer subscriptions;

    public EventExampleClient(ApplicationContext context) throws Exception {
        connections = context.getBean(ConnectionRegistry.class);
        subscriptions = context.getBean(TypedPubSubMappingContainer.class);
        loginUser.setUserId(1001L);
        loginUser.setCategory(DefaultUserCategory.REGISTER.getIdentity());
        loginUser.setDeviceType(DeviceType.PC);
        connections.register(new NettyWebsocketConnection(channel, loginUser));
    }

    public ChatUser getSender() {
        return ChatUser.longUserId(loginUser.getUserId(), loginUser.getCategory());
    }

    public DeviceType getDevice() {
        return loginUser.getDeviceType();
    }

    public Protocol send(Protocol message) throws Throwable {
        Protocol request = new Protocol(message.encode());
        // PING 只有事件字节，发送者和设备使用连接中的可信身份。
        request.setSender(getSender());
        request.setDevice(getDevice());
        subscriptions.getImmediately(request).subscribe(request);

        BinaryWebSocketFrame response = channel.readOutbound();
        if (response == null) {
            throw new IllegalStateException("事件订阅者没有返回消息：" + request.getProtocolType());
        }
        try {
            return new Protocol(response.content().nioBuffer());
        } finally {
            response.release();
        }
    }

    @Override
    public void close() {
        try {
            connections.removeConnection(loginUser);
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
