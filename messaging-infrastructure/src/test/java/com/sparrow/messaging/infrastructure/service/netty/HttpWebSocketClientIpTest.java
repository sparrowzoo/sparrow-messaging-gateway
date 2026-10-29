/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sparrow.messaging.infrastructure.service.netty;

import com.sparrow.support.web.ServletUtility;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HttpWebSocketClientIpTest {
    private static final String PROPERTY = "sparrow.client-ip.trusted-proxies";

    @Test
    void httpAndWebSocketUseTheSameTrustRulesAndHeaderOrder() {
        check("", "203.0.113.10", List.of("192.0.2.99"), List.of("192.0.2.98"), "203.0.113.10");
        check("10.0.0.1,10.0.0.2", "10.0.0.2",
                List.of("192.0.2.99", "198.51.100.7,10.0.0.1"), List.of(), "198.51.100.7");
        check("10.0.0.1", "10.0.0.1", List.of("forged-prefix, 198.51.100.7"), List.of(), "198.51.100.7");
        check("10.0.0.1", "10.0.0.1", List.of(), List.of("198.51.100.7"), "198.51.100.7");
        check("10.0.0.1", "10.0.0.1", List.of("unknown"), List.of("198.51.100.7"), "10.0.0.1");
        check("10.0.0.1", "10.0.0.1", List.of("198.51.100.7,"), List.of(), "10.0.0.1");
        check("10.0.0.1", "10.0.0.1", List.of(), List.of("192.0.2.1", "192.0.2.2"), "10.0.0.1");
        check("2001:db8::1", "2001:db8::1", List.of("2001:db8::7"), List.of(), "2001:db8:0:0:0:0:0:7");
        check("", "2001:db8::7", List.of(), List.of(), "2001:db8:0:0:0:0:0:7");
    }

    @Test
    void invalidLiteralHeadersAreNeverResolvedAsHostNames() {
        for (String invalid : List.of("localhost", "127.1", "999.1.1.1", "198.51.100.7:1234", "", "2001:db8::xyz")) {
            check("10.0.0.1", "10.0.0.1", List.of(invalid), List.of(), "10.0.0.1");
        }
    }

    @Test
    void invalidSharedConfigurationIsRejectedByBothEntrypoints() {
        ServletUtility.getInstance();
        withProperty("localhost", () -> {
            assertThrows(IllegalArgumentException.class, ServletUtility::new);
            assertThrows(IllegalArgumentException.class,
                    () -> new WebSocketServerProtocolSupportHandshake("/websocket", 655360));
        });
    }

    @Test
    void httpKeepsItsMissingRequestAndMissingPeerBehavior() {
        withProperty("", () -> {
            var utility = new ServletUtility();
            assertEquals(com.sparrow.protocol.constant.Constant.LOCALHOST, utility.getClientIp(null));
            assertEquals("", utility.getClientIp(request(null, List.of(), List.of())));
            assertEquals("", utility.getClientIp(request("", List.of(), List.of())));
        });
    }

    @Test
    void removedWebSocketPropertyDoesNotEnableTrust() {
        String oldProperty = "sparrow.messaging.websocket.trusted-proxies";
        String previous = System.getProperty(oldProperty);
        try {
            System.setProperty(oldProperty, "10.0.0.1");
            check(null, "10.0.0.1", List.of("198.51.100.7"), List.of(), "10.0.0.1");
            check("", "10.0.0.1", List.of("198.51.100.7"), List.of(), "10.0.0.1");
        } finally {
            if (previous == null) {
                System.clearProperty(oldProperty);
            } else {
                System.setProperty(oldProperty, previous);
            }
        }
    }

    private void check(String proxies, String peer, List<String> forwarded, List<String> real, String expected) {
        withProperty(proxies, () -> {
            assertEquals(expected, new ServletUtility().getClientIp(request(peer, forwarded, real)), "HTTP");
            var channel = ClientIpTest.channel(peer);
            try {
                var headers = new DefaultHttpHeaders();
                forwarded.forEach(value -> headers.add("X-Forwarded-For", value));
                real.forEach(value -> headers.add("X-Real-IP", value));
                var handshake = new WebSocketServerProtocolHandler.HandshakeComplete("/websocket", headers, null);
                assertEquals(expected, new WebSocketServerProtocolSupportHandshake("/websocket", 655360)
                        .getClientIp(handshake, channel), "WebSocket");
            } finally {
                channel.finishAndReleaseAll();
            }
        });
    }

    private static HttpServletRequest request(String peer, List<String> forwarded, List<String> real) {
        return (HttpServletRequest) Proxy.newProxyInstance(HttpServletRequest.class.getClassLoader(),
                new Class<?>[]{HttpServletRequest.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getRemoteAddr")) {
                        return peer;
                    }
                    if (method.getName().equals("getHeaders") || method.getName().equals("getHeader")) {
                        List<String> values = "X-Forwarded-For".equalsIgnoreCase((String) args[0]) ? forwarded
                                : "X-Real-IP".equalsIgnoreCase((String) args[0]) ? real : List.of();
                        return method.getName().equals("getHeaders") ? Collections.enumeration(values)
                                : values.isEmpty() ? null : values.getFirst();
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static void withProperty(String value, Runnable action) {
        String previous = System.getProperty(PROPERTY);
        try {
            if (value == null) {
                System.clearProperty(PROPERTY);
            } else {
                System.setProperty(PROPERTY, value);
            }
            action.run();
        } finally {
            if (previous == null) {
                System.clearProperty(PROPERTY);
            } else {
                System.setProperty(PROPERTY, previous);
            }
        }
    }
}
