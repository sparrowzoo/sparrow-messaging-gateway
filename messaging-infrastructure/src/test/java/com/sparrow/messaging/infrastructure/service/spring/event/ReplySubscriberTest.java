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

package com.sparrow.messaging.infrastructure.service.spring.event;

import com.sparrow.messaging.domain.service.MultiDeviceConnectionRegistry;
import com.sparrow.messaging.protocol.*;
import com.sparrow.messaging.protocol.api.Subscriber;
import com.sparrow.messaging.protocol.enums.*;
import com.sparrow.protocol.enums.DeviceType;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.*;
import com.sparrow.authenticator.DefaultLoginUser;
import com.sparrow.messaging.domain.service.ClientConnection;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

class ReplySubscriberTest {
    @Test
    void disconnectedUserDoesNotCauseNullPointerInAnyReply() throws Exception {
        for (Subscriber<Protocol> subscriber : replies()) {
            injectEmptyDirectory(subscriber);
            assertDoesNotThrow(() -> subscriber.subscribe(message()));
        }
    }

    @Test
    void missingReplyIdentityHasAnExplicitError() throws Exception {
        for (Subscriber<Protocol> subscriber : replies()) {
            injectEmptyDirectory(subscriber);
            assertThrows(IllegalArgumentException.class, () -> subscriber.subscribe(null));
            assertThrows(IllegalArgumentException.class, () -> subscriber.subscribe(new Protocol()));
        }
    }

    @Test
    void asynchronousSendFailuresAreLoggedWithoutBlockingOrRetrying() throws Exception {
        for (Subscriber<Protocol> subscriber : replies()) {
            var directory = new MultiDeviceConnectionRegistry();
            var completion = new CompletableFuture<Void>();
            var user = new DefaultLoginUser();user.setUserId(1L);user.setCategory(1);user.setDeviceType(DeviceType.PC);
            directory.register(new ClientConnection() {
                public com.sparrow.protocol.LoginUser loginUser() { return user; }
                public CompletionStage<Void> sendMessage(ByteBuffer frame) { return completion; }
                public void closeConnection() {}
                public Long getLastAccessTime() { return 1L; }
                public void refreshAccessTime() {}
                public Long getCreateTime() { return 1L; }
                public Long getDuration() { return 0L; }
            });
            var field = subscriber.getClass().getDeclaredField("connectionRegistry");
            field.setAccessible(true);field.set(subscriber, directory);
            Logger logger = (Logger) LoggerFactory.getLogger(subscriber.getClass());
            Level previous = logger.getLevel();
            var logs = new ListAppender<ILoggingEvent>();logs.start();logger.addAppender(logs);logger.setLevel(Level.WARN);
            try {
                assertTimeoutPreemptively(Duration.ofSeconds(2), () -> subscriber.subscribe(message()));
                assertTrue(logs.list.isEmpty(), "A pending send must not be reported as failed");
                completion.completeExceptionally(new java.io.IOException("test write failure"));
                assertEquals(1, logs.list.size());
                var entry = logs.list.getFirst();
                assertEquals(Level.WARN, entry.getLevel());
                assertTrue(entry.getFormattedMessage().contains("1_1"));
                assertEquals(java.io.IOException.class.getName(), entry.getThrowableProxy().getClassName());
            } finally {
                completion.complete(null);logger.detachAppender(logs);logger.setLevel(previous);logs.stop();
            }
        }
    }

    private List<Subscriber<Protocol>> replies() {
        return List.of(new MessageEventSubscriber(), new PingSubscriber(), new UserStatusEventSubscriber());
    }

    private void injectEmptyDirectory(Subscriber<Protocol> subscriber) throws Exception {
        var field = subscriber.getClass().getDeclaredField("connectionRegistry");
        field.setAccessible(true);field.set(subscriber, new MultiDeviceConnectionRegistry());
    }

    private Protocol message() {
        var message = new Protocol();message.setEventType(DefaultEventType.MESSAGE);
        message.setSender(ChatUser.longUserId(1L, 1));message.setReceiver(ChatUser.longUserId(2L, 1));
        message.setDevice(DeviceType.PC);message.setChatType(ChatType.ONE_2_ONE);message.setClientSendTime(1L);
        return message;
    }
}
