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

import com.sparrow.utility.ClientIpResolver;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;

import java.net.InetAddress;
import java.util.Set;

public class WebSocketServerInitializer extends ChannelInitializer<SocketChannel> {

    private static final String WEBSOCKET_PATH = "/websocket";

    private final SslContext sslCtx;
    private final Set<InetAddress> trustedProxies;

    public WebSocketServerInitializer(SslContext sslCtx) {
        this.sslCtx = sslCtx;
        this.trustedProxies = ClientIpResolver.configuredTrustedProxies();
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        if (sslCtx != null) {
            pipeline.addLast(sslCtx.newHandler(ch.alloc()));
        }
        pipeline.addLast(new HttpServerCodec());
        // 原生 PONG 和业务输入均刷新读计时；连续30秒无输入则关闭连接。
        pipeline.addLast(new IdleStateHandler(30, 0, 0));
        pipeline.addLast(new HttpObjectAggregator(1024));
        // 单帧与分片总量均限制为640 KiB；不启用压缩，避免解压放大。
        pipeline.addLast(new WebSocketFrameAggregator(655360));
        pipeline.addLast("websocket-protocol", new WebSocketServerProtocolSupportHandshake(WEBSOCKET_PATH, 655360, trustedProxies));
        // Netty 安装握手处理器后再插入探测器，才能收到握手完成事件，并在 PONG 被消费前刷新读计时。
        pipeline.addBefore("websocket-protocol", "websocket-heartbeat", new WebSocketHeartbeatHandler());
        // 可选调试首页缺资源时返回404，应用消息由后续网关处理。
        pipeline.addLast(new WebSocketIndexPageHandler(WEBSOCKET_PATH));
        pipeline.addLast(new WebSocketGateway());
    }
}
