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
    void compactNumericSessionKeyMatchesFrontend() {
        assertEquals("011121", ChatSession.create1To1Session(first, second).key());
    }

    @Test
    void base62SessionFormatRejectsUnrepresentableUsers() {
        assertThrows(IllegalArgumentException.class, () ->
                ChatSession.create1To1Session(ChatUser.stringUserId("1", 62), second));
        assertThrows(IllegalArgumentException.class, () ->
                ChatSession.create1To1Session(ChatUser.stringUserId("9".repeat(62), 1), second));
        var boundary = ChatUser.stringUserId("9".repeat(61), 61);
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

    @Test
    void allBase62ChatTypesMatchFrontendKeys() {
        String alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        for (int type = 0; type < alphabet.length(); type++) {
            String key = alphabet.charAt(type) + "group";
            ChatSession session = ChatSession.createSession(type, "group");
            assertEquals(key, session.key());
            ChatSession parsed = ChatSession.parse(key);
            assertEquals(type, parsed.getChatType());
            assertEquals("group", parsed.getId());
            assertEquals(key, parsed.key());
        }
    }

    @Test
    void allBase62UserCategoriesMatchFrontendKeysInBothPositions() {
        String alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        for (int category = 0; category < alphabet.length(); category++) {
            ChatUser big = ChatUser.stringUserId("z", category);
            ChatUser small = ChatUser.stringUserId("1", category);
            String key = "0" + alphabet.charAt(category) + alphabet.charAt(category) + "1z1";
            assertEquals(key, ChatSession.create1To1Session(big, small).key());
            assertEquals(key, ChatSession.create1To1Session(small, big).key());
            ChatSession parsed = ChatSession.parse(key);
            assertEquals(big, parsed.get1To1Members().getFirst());
            assertEquals(small, parsed.get1To1Members().getSecond());
            assertEquals(small, parsed.getOppositeUser(big));
        }
    }

    @Test
    void everyUserIdLengthUsesOneBase62Character() {
        String alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        for (int length = 1; length <= 61; length++) {
            ChatUser big = ChatUser.stringUserId("z".repeat(length), 9);
            ChatUser small = ChatUser.stringUserId("1", 0);
            String key = "090" + alphabet.charAt(length) + big.getId() + "1";
            assertEquals(key, ChatSession.create1To1Session(big, small).key());
            assertEquals(big, ChatSession.parse(key).get1To1Members().getFirst());
            assertEquals(small, ChatSession.parse(key).get1To1Members().getSecond());
        }
    }

    @Test
    void uppercaseAndLowercaseCategoriesSortNumericallyForEqualIds() {
        ChatUser upper = ChatUser.stringUserId("same", 35);
        ChatUser lower = ChatUser.stringUserId("same", 36);
        assertEquals("0aZ4samesame", ChatSession.create1To1Session(upper, lower).key());
        assertEquals("0aZ4samesame", ChatSession.create1To1Session(lower, upper).key());
        assertEquals(lower, ChatSession.parse("0aZ4samesame").get1To1Members().getFirst());
    }

    @Test
    void malformedBase62FieldsAreRejected() {
        for (String key : new String[]{"-group", "_group", ":group", " group", "群group", "A", "0"}) {
            assertThrows(IllegalArgumentException.class, () -> ChatSession.parse(key), key);
        }
        for (String id : new String[]{"", "111x", "110ab", "11-ab", "11_ab", "11:ab",
                "-11ab", "1_1ab", "_11ab", "1-1ab", "112ab", "111a", "111a" + "b".repeat(62)}) {
            assertThrows(IllegalArgumentException.class,
                    () -> ChatSession.createSession(0, id).get1To1Members(), id);
        }
        for (int category : new int[]{-1, 62}) {
            assertThrows(IllegalArgumentException.class,
                    () -> ChatSession.create1To1Session(ChatUser.stringUserId("1", category), second));
            assertThrows(IllegalArgumentException.class,
                    () -> ChatSession.createSession(category, "group").key());
        }
        assertNull(ChatSession.parse(null));
        assertNull(ChatSession.parse(""));
        assertThrows(IllegalArgumentException.class, () -> ChatSession.createSession(1, "").key());
    }
}
