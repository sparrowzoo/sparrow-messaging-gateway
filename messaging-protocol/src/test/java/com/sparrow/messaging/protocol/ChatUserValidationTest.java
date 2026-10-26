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

import org.junit.jupiter.api.Test;
import java.util.HashSet;
import static org.junit.jupiter.api.Assertions.*;

class ChatUserValidationTest {
    @Test
    void equalIdentitiesWorkThroughObjectAndCollections() {
        var first = ChatUser.stringUserId("1", 1);
        var second = ChatUser.stringUserId("1", 1);
        assertEquals((Object) first, second);
        assertEquals(first.hashCode(), second.hashCode());
        var users = new HashSet<ChatUser>();users.add(first);
        assertTrue(users.contains(second));
        assertFalse(first.equals(ChatUser.stringUserId("1", 2)));
        assertFalse(first.equals((Object) "1"));
    }

    @Test
    void userKeyWithUnderscoreRoundTrips() {
        var user = ChatUser.stringUserId("customer_a_b", 2);
        assertEquals(user, ChatUser.parse(user.key()));
        assertNull(ChatUser.parse(null));
        assertNull(ChatUser.parse("invalid"));
    }

    @Test
    void wireUserEncodingChecksByteLengthAndCategory() {
        var valid = ChatUser.stringUserId("a".repeat(255), 255).toBytes();
        assertEquals(255, Byte.toUnsignedInt(valid[0]));
        assertEquals(255, Byte.toUnsignedInt(valid[valid.length - 1]));
        assertThrows(IllegalArgumentException.class, () -> ChatUser.stringUserId("a".repeat(256), 1).toBytes());
        assertThrows(IllegalArgumentException.class, () -> ChatUser.stringUserId("中".repeat(86), 1).toBytes());
        assertThrows(IllegalArgumentException.class, () -> ChatUser.stringUserId("1", 256).toBytes());
        assertThrows(IllegalArgumentException.class, () -> ChatUser.stringUserId("1", null).toBytes());
        assertThrows(IllegalArgumentException.class, () -> ChatUser.stringUserId(null, 1).toBytes());
        var unknownCategory = ChatUser.stringUserId("1", null);
        assertFalse(unknownCategory.isVisitor());
        assertFalse(unknownCategory.isAI());
        assertThrows(IllegalArgumentException.class, unknownCategory::key);
    }
}
