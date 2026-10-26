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

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.junit.jupiter.api.Test;
import java.net.*;
import static org.junit.jupiter.api.Assertions.*;

class ClientIpTest {
    @Test
    void directClientCannotOverrideItsSocketAddress() {
        var channel = channel("203.0.113.10");
        try {
            var headers = new DefaultHttpHeaders().set("X-Real-IP", "127.0.0.1")
                    .set("X-Forwarded-For", "192.0.2.1");
            var request = new WebSocketServerProtocolHandler.HandshakeComplete("/websocket", headers, null);
            assertEquals("203.0.113.10", new WebSocketServerProtocolSupportHandshake("/websocket", 655360)
                    .getClientIp(request, channel));
        } finally { channel.finishAndReleaseAll(); }
    }

    @Test
    void loopbackIsNotImplicitlyATrustedProxy() {
        var channel = channel("127.0.0.1");
        try {
            var headers = new DefaultHttpHeaders().set("X-Real-IP", "192.0.2.1");
            var request = new WebSocketServerProtocolHandler.HandshakeComplete("/websocket", headers, null);
            assertEquals("127.0.0.1", new WebSocketServerProtocolSupportHandshake("/websocket", 655360)
                    .getClientIp(request, channel));
        } finally { channel.finishAndReleaseAll(); }
    }

    @Test
    void configuredProxyCanProvideRealIp() {
        assertEquals("198.51.100.7", resolve("10.0.0.1", "10.0.0.1", null, "198.51.100.7"));
    }

    @Test
    void forwardedChainIsWalkedFromTheTrustedRightHandSide() {
        assertEquals("198.51.100.7", resolve("10.0.0.2", "10.0.0.1,10.0.0.2",
                "192.0.2.99,198.51.100.7,10.0.0.1", "127.0.0.1"));
        assertEquals("198.51.100.7", resolve("10.0.0.2", "10.0.0.2",
                "forged-prefix,198.51.100.7", null));
    }

    @Test
    void malformedTraversedHeadersFallBackToTheSocketAndDoNotUseAnotherHeader() {
        assertEquals("10.0.0.1", resolve("10.0.0.1", "10.0.0.1", "unknown", "198.51.100.7"));
        assertEquals("10.0.0.1", resolve("10.0.0.1", "10.0.0.1", "198.51.100.7,", null));
        assertEquals("10.0.0.1", resolve("10.0.0.1", "10.0.0.1", null, "localhost"));
        assertEquals("10.0.0.1", resolve("10.0.0.1", "10.0.0.1", null, null));
    }

    @Test
    void untrustedPeerCannotUseAChainContainingTrustedAddresses() {
        assertEquals("203.0.113.10", resolve("203.0.113.10", "10.0.0.1",
                "198.51.100.7,10.0.0.1", "192.0.2.7"));
    }

    @Test
    void literalIpv6AddressesAreSupportedWithoutDns() throws Exception {
        assertEquals(InetAddress.getByName("2001:db8::7").getHostAddress(),
                resolve("2001:db8::1", "2001:db8::1", "2001:db8::7", null));
    }

    @Test
    void invalidProxyConfigurationFailsBeforeOpeningAServer() {
        assertThrows(IllegalArgumentException.class,
                () -> WebSocketServerProtocolSupportHandshake.parseTrustedProxies("localhost"));
        assertThrows(IllegalArgumentException.class,
                () -> WebSocketServerProtocolSupportHandshake.parseTrustedProxies("10.0.0.0/8"));
        assertThrows(IllegalArgumentException.class,
                () -> WebSocketServerProtocolSupportHandshake.parseTrustedProxies("10.0.0.1,"));
        assertTrue(WebSocketServerProtocolSupportHandshake.parseTrustedProxies("").isEmpty());
    }

    @Test
    void repeatedForwardedHeadersPreserveTheWholeProxyChain() {
        var channel = channel("10.0.0.1");
        try {
            var headers = new DefaultHttpHeaders().add("X-Forwarded-For", "192.0.2.99")
                    .add("X-Forwarded-For", "198.51.100.7");
            var request = new WebSocketServerProtocolHandler.HandshakeComplete("/websocket", headers, null);
            var handler = new WebSocketServerProtocolSupportHandshake("/websocket", 655360,
                    WebSocketServerProtocolSupportHandshake.parseTrustedProxies("10.0.0.1"));
            assertEquals("198.51.100.7", handler.getClientIp(request, channel));
        } finally { channel.finishAndReleaseAll(); }
    }

    @Test
    void ambiguousRepeatedRealIpHeadersFallBackToTheSocket() {
        var channel = channel("10.0.0.1");
        try {
            var headers = new DefaultHttpHeaders().add("X-Real-IP", "192.0.2.99")
                    .add("X-Real-IP", "198.51.100.7");
            var request = new WebSocketServerProtocolHandler.HandshakeComplete("/websocket", headers, null);
            var handler = new WebSocketServerProtocolSupportHandshake("/websocket", 655360,
                    WebSocketServerProtocolSupportHandshake.parseTrustedProxies("10.0.0.1"));
            assertEquals("10.0.0.1", handler.getClientIp(request, channel));
        } finally { channel.finishAndReleaseAll(); }
    }

    private String resolve(String socketIp, String proxies, String forwarded, String real) {
        var channel = channel(socketIp);
        try {
            var headers = new DefaultHttpHeaders();
            if (forwarded != null) { headers.set("X-Forwarded-For", forwarded); }
            if (real != null) { headers.set("X-Real-IP", real); }
            var request = new WebSocketServerProtocolHandler.HandshakeComplete("/websocket", headers, null);
            var handler = new WebSocketServerProtocolSupportHandshake("/websocket", 655360,
                    WebSocketServerProtocolSupportHandshake.parseTrustedProxies(proxies));
            return handler.getClientIp(request, channel);
        } finally { channel.finishAndReleaseAll(); }
    }

    static EmbeddedChannel channel(String ip) {
        return new EmbeddedChannel() {
            @Override protected SocketAddress remoteAddress0() {
                return new InetSocketAddress(ip, 12345);
            }
        };
    }
}
