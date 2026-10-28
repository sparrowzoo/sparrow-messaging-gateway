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

package com.sparrow.messaging.messaging.example.websocket;

import com.alibaba.fastjson.JSON;
import com.sparrow.messaging.protocol.Protocol;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * JDK 真实 WebSocket 客户端，等待认证通知后发送二进制协议报文。
 */
public class RealWebSocketClient implements WebSocket.Listener, AutoCloseable {
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final CompletableFuture<Void> authenticated = new CompletableFuture<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private final BlockingQueue<Object> responses = new LinkedBlockingQueue<>();
    private final ByteArrayOutputStream binary = new ByteArrayOutputStream();
    private final StringBuilder text = new StringBuilder();
    private WebSocket socket;

    public RealWebSocketClient(URI uri, String token) throws Exception {
        try {
            socket = httpClient.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(5))
                    .subprotocols(token).buildAsync(uri, this).get(5, TimeUnit.SECONDS);
            authenticated.get(5, TimeUnit.SECONDS);
        } catch (Exception exception) {
            close();
            throw exception;
        }
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        text.append(data);
        if (last) {
            try {
                var result = JSON.parseObject(text.toString());
                if ("AUTH_BIND".equals(result.getString("instruction")) && result.getBooleanValue("success")) {
                    authenticated.complete(null);
                } else {
                    authenticated.completeExceptionally(new IllegalStateException("WebSocket 认证未通过"));
                }
            } catch (Exception exception) {
                authenticated.completeExceptionally(exception);
            }
            text.setLength(0);
        }
        webSocket.request(1);
        return null;
    }

    @Override
    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        binary.writeBytes(bytes);
        if (last) {
            try {
                responses.add(new Protocol(binary.toByteArray()));
            } catch (Exception exception) {
                responses.add(exception);
            }
            binary.reset();
        }
        webSocket.request(1);
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        authenticated.completeExceptionally(error);
        responses.offer(error);
        closed.completeExceptionally(error);
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        var error = new IllegalStateException("WebSocket 已关闭：" + statusCode);
        authenticated.completeExceptionally(error);
        responses.offer(error);
        closed.complete(null);
        return null;
    }

    public Protocol send(Protocol message) throws Exception {
        socket.sendBinary(message.encode(), true).get(5, TimeUnit.SECONDS);
        Object response = responses.poll(5, TimeUnit.SECONDS);
        if (response instanceof Protocol protocol) {
            return protocol;
        }
        if (response instanceof Throwable error) {
            throw new IllegalStateException("WebSocket 收发失败", error);
        }
        throw new IllegalStateException("等待 WebSocket 响应超时：" + message.getProtocolType());
    }

    @Override
    public void close() {
        if (socket != null) {
            try {
                socket.sendClose(WebSocket.NORMAL_CLOSURE, "example complete").get(5, TimeUnit.SECONDS);
                closed.get(5, TimeUnit.SECONDS);
            } catch (Exception exception) {
                socket.abort();
            }
        }
        httpClient.close();
    }
}
