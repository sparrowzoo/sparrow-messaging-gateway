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
import static org.junit.jupiter.api.Assertions.*;

class MessageUniqueIdentifyTest {
    @Test
    void distinctUserFieldsCannotCollide() {
        var first = new MessageUniqueIdentify(ChatUser.stringUserId("23", 1), 100L);
        var second = new MessageUniqueIdentify(ChatUser.stringUserId("3", 12), 100L);
        assertNotEquals(first.key(), second.key());
    }

    @Test
    void legacyTimestampIdentityRemainsDeterministic() {
        var user = ChatUser.stringUserId("a-b", 1);
        assertEquals(new MessageUniqueIdentify(user, 100L).key(), new MessageUniqueIdentify(user, 100L).key());
    }

    @Test
    void explicitMessageIdSeparatesSameMillisecondAndRebuildsDeterministically() {
        var user = ChatUser.stringUserId("a-b", 1);
        var first = new MessageUniqueIdentify(user, 100L, "device1-message1");
        var second = new MessageUniqueIdentify(user, 100L, "device2-message1");
        assertNotEquals(first.key(), second.key());
        assertEquals(first.key(), new MessageUniqueIdentify(user, 100L, "device1-message1").key());
        assertNotEquals(first.key(), new MessageUniqueIdentify(user, 100L).key());
    }

    @Test
    void variableLengthUserAndMessageIdsHaveUnambiguousBoundaries() {
        assertNotEquals(new MessageUniqueIdentify(ChatUser.stringUserId("a-b", 1), 100L, "c").key(),
                new MessageUniqueIdentify(ChatUser.stringUserId("a", 1), 100L, "b-c").key());
    }

    @Test
    void missingOrInvalidIdentityIsRejectedExplicitly() {
        assertThrows(IllegalArgumentException.class, () -> new MessageUniqueIdentify(null, 100L));
        assertThrows(IllegalArgumentException.class, () -> new MessageUniqueIdentify(ChatUser.stringUserId("1", 1), null));
        assertThrows(IllegalArgumentException.class, () -> new MessageUniqueIdentify(ChatUser.stringUserId("1", 1), -1L));
        assertThrows(IllegalArgumentException.class, () -> new MessageUniqueIdentify(ChatUser.stringUserId("1", 1), 100L, ""));
    }
}
