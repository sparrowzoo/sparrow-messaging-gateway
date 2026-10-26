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

import com.sparrow.messaging.protocol.enums.ChatType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ChatSessionTest {
    private final ChatUser first = ChatUser.stringUserId("1", 1);
    private final ChatUser second = ChatUser.stringUserId("2", 1);

    @Test
    void onlyParticipantsAreMembersAndHaveAnOppositeUser() {
        var session = ChatSession.create1To1Session(first, second);
        assertTrue(session.isOne2OneMember(first));
        assertTrue(session.isOne2OneMember(second));
        assertEquals(second.key(), session.getOppositeUser(first).key());
        assertEquals(first.key(), session.getOppositeUser(second).key());
        var outsider = ChatUser.stringUserId("999", 1);
        assertFalse(session.isOne2OneMember(outsider));
        assertNull(session.getOppositeUser(outsider));
        assertFalse(session.isOne2OneMember(null));
        assertNull(session.getOppositeUser(null));
    }

    @Test
    void exchangingSameIdDifferentCategoriesKeepsTheSessionKey() {
        var user = ChatUser.stringUserId("42", 1);
        var ai = ChatUser.stringUserId("42", 2);
        assertEquals(ChatSession.create1To1Session(user, ai).key(),
                ChatSession.create1To1Session(ai, user).key());
    }

    @Test
    void existingNumericSessionKeyFormatIsPreserved() {
        assertEquals("0110121", ChatSession.create1To1Session(first, second).key());
    }

    @Test
    void oldSessionFormatRejectsUnrepresentableUsers() {
        assertThrows(IllegalArgumentException.class, () ->
                ChatSession.create1To1Session(ChatUser.stringUserId("1", 12), second));
        assertThrows(IllegalArgumentException.class, () ->
                ChatSession.create1To1Session(ChatUser.stringUserId("9".repeat(100), 1), second));
        var boundary = ChatUser.stringUserId("9".repeat(99), 9);
        assertTrue(ChatSession.create1To1Session(boundary, second).isOne2OneMember(boundary));
    }

    @Test
    void malformedSingleChatKeyHasAnExplicitError() {
        assertThrows(IllegalArgumentException.class, () -> ChatSession.parse("0bad").get1To1Members());
    }

    @Test
    void groupHasNoOneToOneOpposite() {
        var group = ChatSession.createSession(ChatType.GROUP.getIdentity(), "group");
        assertFalse(group.isOne2OneMember(first));
        assertNull(group.getOppositeUser(first));
    }
}
