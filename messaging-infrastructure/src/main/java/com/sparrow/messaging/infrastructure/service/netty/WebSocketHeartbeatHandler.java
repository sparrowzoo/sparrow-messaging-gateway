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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

/** Sends native probes after ten seconds without input; the separate read timeout closes dead connections. */
final class WebSocketHeartbeatHandler extends IdleStateHandler {
    private boolean handshakeComplete;

    WebSocketHeartbeatHandler() {
        super(10, 0, 0);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
            handshakeComplete = true;
        }
        super.userEventTriggered(ctx, event);
    }

    @Override
    protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent event) {
        // Only this handler's ten-second events are consumed here; upstream timeout events still propagate.
        if (handshakeComplete) {
            ctx.writeAndFlush(new PingWebSocketFrame());
        }
    }
}
