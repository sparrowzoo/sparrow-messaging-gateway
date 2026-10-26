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

package com.sparrow.messaging.protocol;

import com.sparrow.messaging.protocol.enums.*;
import com.sparrow.protocol.enums.DeviceType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ProtocolValidationTest {
    @Test
    void missingRequiredTimestampHasAnExplicitError() {
        var ack = new Protocol();
        ack.setEventType(DefaultEventType.ACK);
        ack.setDevice(DeviceType.PC);
        ack.setChatType(ChatType.ONE_2_ONE);
        var error = assertThrows(IllegalArgumentException.class, ack::encode);
        assertTrue(error.getMessage().contains("clientSendTime"));
    }

    @Test
    void eventOnlyAndContentOnlyMessagesDoNotRequireTimestamp() {
        assertDoesNotThrow(() -> Protocol.pong().encode());
        var status = new Protocol();status.setEventType(DefaultEventType.USER_STATUS);
        status.setDevice(DeviceType.PC);status.setChatType(ChatType.ONE_2_ONE);
        assertDoesNotThrow(status::encode);
    }
    @Test
    void negativeClientOrServerTimestampIsRejectedBeforeSending() {
        var ack = timedAck();ack.setClientSendTime(-1L);
        assertThrows(IllegalArgumentException.class, ack::encode);
        ack.setClientSendTime(1L);ack.setServerTime(-1L);
        assertThrows(IllegalArgumentException.class, ack::encode);
    }

    @Test
    void singleChatUnsupportedIdentityIsRejectedByEncoderAndDecoder() {
        assertThrows(IllegalArgumentException.class, () -> message(ChatUser.stringUserId("1", 12)).encode());
        assertThrows(IllegalArgumentException.class, () -> message(ChatUser.stringUserId("9".repeat(100), 1)).encode());
        var encoded = message(ChatUser.stringUserId("1", 1)).encode();
        var bytes = new byte[encoded.remaining()];encoded.get(bytes);
        bytes[bytes.length - 4] = 12;
        assertThrows(IllegalArgumentException.class, () -> new Protocol(bytes));
    }

    @Test
    void settingBothParticipantsBuildsTheSameSessionRegardlessOfSetterOrder() {
        var message = message(ChatUser.stringUserId("1", 1));
        assertEquals("0110121", message.getChatSession().key());
        var reverse = new Protocol();
        reverse.setReceiver(ChatUser.stringUserId("2", 1));
        reverse.setSender(ChatUser.stringUserId("1", 1));
        reverse.setChatType(ChatType.ONE_2_ONE);
        assertNotNull(reverse.getChatSession());
        assertEquals("0110121", reverse.getChatSession().key());
    }

    private Protocol timedAck() {
        var ack = new Protocol();ack.setEventType(DefaultEventType.ACK);
        ack.setDevice(DeviceType.PC);ack.setChatType(ChatType.ONE_2_ONE);ack.setClientSendTime(1L);
        return ack;
    }

    private Protocol message(ChatUser sender) {
        var message = timedAck();message.setEventType(DefaultEventType.IM);
        message.setSender(sender);message.setReceiver(ChatUser.stringUserId("2", 1));
        return message;
    }
}
