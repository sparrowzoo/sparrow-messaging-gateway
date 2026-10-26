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

package com.sparrow.messaging.messaging.example.typed.file;

import com.sparrow.messaging.protocol.enums.DefaultEventType;
import com.sparrow.messaging.protocol.EventType;
import com.sparrow.messaging.infrastructure.service.spring.AbstractSpringTypedSubscriber;
import com.sparrow.messaging.messaging.example.typed.ExampleMessageType;
import com.sparrow.messaging.protocol.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

@Component
public class FileMessageSubscriber extends AbstractSpringTypedSubscriber<FileMessage> {
    private static final Logger logger = LoggerFactory.getLogger(FileMessageSubscriber.class);
    private final AtomicReference<FileMessage> lastMessage = new AtomicReference<>();

    @Override
    public EventType getEventType() {
        return DefaultEventType.IM;
    }

    @Override
    public MessageType getMessageType() {
        return ExampleMessageType.FILE;
    }

    @Override
    public void subscribe(FileMessage message) {
        lastMessage.set(message);
        logger.info("Typed 文件示例订阅收到消息：{}", message.text());
    }

    public FileMessage getLastMessage() {
        return lastMessage.get();
    }
}
