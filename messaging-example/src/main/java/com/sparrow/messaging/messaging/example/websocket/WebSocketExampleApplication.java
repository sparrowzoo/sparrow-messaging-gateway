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

import com.sparrow.authenticator.AuthenticationInfo;
import com.sparrow.authenticator.Authenticator;
import com.sparrow.authenticator.DefaultLoginUser;
import com.sparrow.authenticator.HostAuthenticationToken;
import com.sparrow.authenticator.enums.AuthenticatorError;
import com.sparrow.messaging.messaging.example.typed.ExampleMessageType;
import com.sparrow.messaging.protocol.ChatUser;
import com.sparrow.messaging.protocol.Protocol;
import com.sparrow.messaging.protocol.enums.ChatType;
import com.sparrow.messaging.protocol.enums.DefaultEventType;
import com.sparrow.messaging.starter.EnableMessage;
import com.sparrow.protocol.BusinessException;
import com.sparrow.protocol.LoginUser;
import com.sparrow.protocol.enums.DeviceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableMessage
public class WebSocketExampleApplication {
    public static final String TOKEN = "websocket-example-token";
    private static final Logger logger = LoggerFactory.getLogger(WebSocketExampleApplication.class);

    public static void main(String[] args) throws Exception {
        try (var context = SpringApplication.run(WebSocketExampleApplication.class, args)) {
            Exchange result = runExample(context);
            logger.info("真实 WebSocket 发送 PING，收到 eventType={}", result.pong().getEventType());
            logger.info("真实 WebSocket 发送 MESSAGE，收到 eventType={}", result.ack().getEventType());
        }
    }

    public static Exchange runExample(ApplicationContext context) throws Exception {
        try (var server = new ExistingWebSocketServer(context);
                var client = new RealWebSocketClient(server.uri(), TOKEN)) {
            Protocol ping = new Protocol();
            ping.setEventType(DefaultEventType.PING);
            Protocol pong = client.send(ping);

            Protocol message = new Protocol();
            message.setEventType(DefaultEventType.IM);
            message.setDevice(DeviceType.PC);
            message.setChatType(ChatType.ONE_2_ONE);
            message.setMessageType(ExampleMessageType.GREETING.getIdentity().shortValue());
            message.setReceiver(ChatUser.longUserId(1002L, LoginUser.CATEGORY_REGISTER));
            // 网关必须用认证用户覆盖客户端填写的发送者。
            message.setSender(ChatUser.longUserId(9999L, LoginUser.CATEGORY_REGISTER));
            message.setPayload("Hello real WebSocket");
            message.setClientSendTime(System.currentTimeMillis());
            Protocol ack = client.send(message);
            return new Exchange(pong, ack, context.getBean(WebSocketMessageSubscriber.class).getLastMessage());
        }
    }

    @Bean
    public Authenticator exampleAuthenticator() {
        return new Authenticator() {
            @Override
            public String login(AuthenticationInfo info) {
                throw new UnsupportedOperationException("示例只演示已有 token 的握手认证");
            }

            @Override
            public LoginUser authenticate(HostAuthenticationToken token) throws BusinessException {
                if (!TOKEN.equals(token.getCredential())) {
                    throw new BusinessException(AuthenticatorError.USER_TOKEN_ABNORMAL);
                }
                DefaultLoginUser user = new DefaultLoginUser();
                user.setUserId(1001L);
                user.setCategory(LoginUser.CATEGORY_REGISTER);
                user.setDeviceType(DeviceType.PC);
                return user;
            }
        };
    }

    public record Exchange(Protocol pong, Protocol ack, Protocol published) {
    }
}
