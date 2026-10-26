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

import com.alibaba.fastjson.JSON;
import com.sparrow.authenticator.Authenticator;
import com.sparrow.authenticator.enums.AuthenticatorError;
import com.sparrow.authenticator.token.BearerToken;
import com.sparrow.core.spi.ApplicationContext;
import com.sparrow.messaging.domain.service.ClientConnection;
import com.sparrow.messaging.domain.service.ConnectionRegistry;
import com.sparrow.protocol.BusinessException;
import com.sparrow.protocol.LoginUser;
import com.sparrow.protocol.Result;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.NetUtil;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
/**
 * WebSocketServerProtocolHandshakeHandler
 */
public class WebSocketServerProtocolSupportHandshake extends WebSocketServerProtocolHandler {
    public static final String BIND = "AUTH_BIND";
    private ClientConnection connection;
    private final Set<InetAddress> trustedProxies;

    // js 不支持ping pong 帧
    // https://stackoverflow.com/questions/10585355/sending-websocket-ping-pong-frame-from-browser

    /**
     * FIN: 1 bit，表示该帧是否为消息的最后一帧。1-是，0-否。
     * <p>
     * <p>
     * RSV1,RSV2,RSV3: 1 bit each，预留(3位)，扩展的预留标志。一般情况为0，除非协商的扩展定义为非零值。如果接收到非零值且不为协商扩展定义，接收端必须使连接失败。
     * <p>
     * <p>
     * Opcode: 4 bits，定义消息帧的操作类型，如果接收到一个未知Opcode，接收端必须使连接失败。
     * <p>
     * （0x0-延续帧，0x1-文本帧，0x2-二进制帧，0x8-关闭帧，0x9-PING帧，0xA-PONG帧
     * <p>
     * （在接收到PING帧时，终端必须发送一个PONG帧响应，除非它已经接收到关闭帧）
     * <p>
     * 0x3-0x7保留给未来的非控制帧，0xB-F保留给未来的控制帧）
     * <p>
     * <p>
     * Mask: 1 bit，表示该帧是否为隐藏的，即被加密保护的。1-是，0-否。Mask=1时，必须传一个Masking-key，用于解除隐藏（客户端发送消息给服务器端，Mask必须为1）。
     * <p>
     * Payload length: 7 bits, 7+16 bits, or 7+64 bits，有效载荷数据的长度（扩展数据长度+应用数据长度，扩展数据长度可以为0）。
     * <p>
     * <p>
     * Masking-key: 0 or 4 bytes，用于解除帧隐藏（加密）的key，Mask=1时不为空，Mask=0时不用传。
     * <p>
     * <p>
     * Payload data: (x+y) bytes，有效载荷数据包括扩展数据（x bytes）和应用数据（y bytes）。有效载荷数据是用户真正要传输的数据。
     *
     * @param websocketPath
     * @param maxLength
     */
    public WebSocketServerProtocolSupportHandshake(String websocketPath, Integer maxLength) {
        this(websocketPath, maxLength, Set.of());
    }

    public WebSocketServerProtocolSupportHandshake(String websocketPath, Integer maxLength,
                                                   Set<InetAddress> trustedProxies) {
        super(websocketPath, "*", false, maxLength);
        this.trustedProxies = trustedProxies == null ? Set.of() : Set.copyOf(trustedProxies);
    }

    /** 只对明确列出的代理采信转发头；不解析主机名，也不隐式信任内网或环回地址。 */
    static Set<InetAddress> parseTrustedProxies(String configured) {
        if (configured == null || configured.isBlank()) {
            return Set.of();
        }
        Set<InetAddress> addresses = new HashSet<>();
        for (String value : configured.split(",", -1)) {
            InetAddress address = literalAddress(value);
            if (address == null) {
                throw new IllegalArgumentException("Trusted proxy must be a literal IP address: " + value);
            }
            addresses.add(address);
        }
        return Set.copyOf(addresses);
    }

    private static InetAddress literalAddress(String value) {
        if (value == null) {
            return null;
        }
        byte[] bytes = NetUtil.createByteArrayFromIpAddressString(value.trim());
        if (bytes == null) {
            return null;
        }
        try {
            return InetAddress.getByAddress(bytes);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Invalid IP address", e);
        }
    }

    public String getClientIp(HandshakeComplete handshake, Channel channel) {
        if (!(channel.remoteAddress() instanceof InetSocketAddress socket) || socket.getAddress() == null) {
            throw new IllegalArgumentException("Client socket address is unavailable");
        }
        InetAddress peer = socket.getAddress();
        String socketIp = peer.getHostAddress();
        if (!trustedProxies.contains(peer)) {
            return socketIp;
        }
        HttpHeaders headers = handshake.requestHeaders();
        List<String> forwardedHeaders = headers.getAll("X-Forwarded-For");
        if (!forwardedHeaders.isEmpty()) {
            String forwarded = String.join(",", forwardedHeaders);
            String[] hops = forwarded.split(",", -1);
            InetAddress client = peer;
            // 从直连代理向左回溯；一旦遇到不可信来源，就不再相信其提供的前缀。
            for (int i = hops.length - 1; i >= 0 && trustedProxies.contains(client); i--) {
                InetAddress hop = literalAddress(hops[i]);
                if (hop == null) {
                    return socketIp;
                }
                client = hop;
            }
            return client.getHostAddress();
        }
        List<String> realIpHeaders = headers.getAll("X-Real-IP");
        InetAddress realIp = realIpHeaders.size() == 1 ? literalAddress(realIpHeaders.getFirst()) : null;
        return realIp == null ? socketIp : realIp.getHostAddress();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof HandshakeComplete) {
            ConnectionRegistry connectionRegistry = ApplicationContext.getContainer().getBean(ConnectionRegistry.class);
            HandshakeComplete serverHandshakeComplete = (HandshakeComplete) evt;
            //token 必须要url编码 否则会报invalid token
            //编解码移至Authenticator
            String token = serverHandshakeComplete.requestHeaders().get("sec-websocket-protocol");
            try {
                String ip = this.getClientIp(serverHandshakeComplete, ctx.channel());
                LoginUser loginUser = ApplicationContext.getContainer().getBean(Authenticator.class).authenticate(new BearerToken(token, ip));
                this.connection = new NettyWebsocketConnection(ctx.channel(), loginUser);
                ClientConnection old = connectionRegistry.register(this.connection);
                if (old != null) {
                    old.closeConnection();
                }
                Result<LoginUser> result = Result.success(loginUser, BIND);
                String userInfo = JSON.toJSONString(result);
                ctx.channel().writeAndFlush(new TextWebSocketFrame(userInfo));
            } catch (Exception e) {
                log.error("authenticate error", e);
                Result result = null;
                if (e instanceof BusinessException businessException) {
                    result = Result.fail(businessException);
                } else {
                    result = Result.fail(AuthenticatorError.USER_TOKEN_ABNORMAL);
                }
                result.setInstruction(BIND);
                ctx.channel().writeAndFlush(new TextWebSocketFrame(JSON.toJSONString(result))).addListener(future -> {
                    ctx.channel().close();
                });
            }
            super.userEventTriggered(ctx, evt);
            return;
        }
        if (evt instanceof IdleStateEvent) {
            ctx.close();
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        try {
            if (this.connection != null) {
                ConnectionRegistry registry = ApplicationContext.getContainer().getBean(ConnectionRegistry.class);
                LoginUser user = this.connection.loginUser();
                // channelInactive 触发时底层连接已断开，这里调用 registry 是为了清理 Map 中的记录。
                // 仅在记录仍指向当前连接时移除，避免旧连接的回调误删新连接。
                registry.removeConnection(user, this.connection);
            }
        } finally {
            super.channelInactive(ctx);
        }
    }
}
