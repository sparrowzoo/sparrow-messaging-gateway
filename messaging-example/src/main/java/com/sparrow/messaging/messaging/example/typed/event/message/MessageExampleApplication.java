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

package com.sparrow.messaging.messaging.example.typed.event.message;

import com.sparrow.messaging.messaging.example.typed.event.EventExampleClient;
import com.sparrow.messaging.protocol.ChatUser;
import com.sparrow.messaging.protocol.Protocol;
import com.sparrow.messaging.protocol.enums.ChatType;
import com.sparrow.messaging.protocol.enums.DefaultEventType;
import com.sparrow.messaging.starter.EnableMessage;
import com.sparrow.protocol.LoginUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
@EnableMessage
public class MessageExampleApplication {
    private static final Logger logger = LoggerFactory.getLogger(MessageExampleApplication.class);

    public static void main(String[] args) throws Throwable {
        try (var context = SpringApplication.run(MessageExampleApplication.class, args)) {
            Protocol ack = sendMessage(context);
            logger.info("发送 MESSAGE，收到 eventType={}", ack.getEventType());
        }
    }

    public static Protocol sendMessage(ApplicationContext context) throws Throwable {
        try (var client = new EventExampleClient(context)) {
            Protocol message = new Protocol();
            message.setEventType(DefaultEventType.IM);
            message.setDevice(client.getDevice());
            message.setChatType(ChatType.ONE_2_ONE);
            message.setMessageType((short) 1);
            message.setReceiver(ChatUser.longUserId(1002L, LoginUser.CATEGORY_REGISTER));
            message.setSender(client.getSender());
            message.setPayload("Hello ACK");
            message.setClientSendTime(System.currentTimeMillis());
            return client.send(message);
        }
    }
}
