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
import com.sparrow.protocol.Pair;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 同一对单聊参与者，无论发送方向如何，都使用同一个会话 key。
 * key 格式：聊天类型一位 + 用户类别各一位 + 较大用户 ID 的字符长度一位 + 双方用户 ID。
 * 类型、类别和长度统一使用 0-9A-Za-z；单聊类别限 0–61，双方 ID 的 String.length() 限 1–61。
 * 与前端使用相同字符表和排序规则；超出范围明确拒绝，不截断。
 */
@Data
@NoArgsConstructor
public class ChatSession {
    private static final String BASE62_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";


    public static ChatSession createSession(Integer chatType, String id) {
        return new ChatSession(chatType, null, null, id);
    }

    public static ChatSession create1To1Session(ChatUser sender, ChatUser receiver) {
        return new ChatSession(ChatType.ONE_2_ONE.getIdentity(), sender, receiver, null);
    }

    private ChatSession(int chatType, ChatUser sender, ChatUser receiver, String id) {
        this.chatType = chatType;
        if (id != null) {
            this.id = id;
            return;
        }
        if (chatType == ChatType.ONE_2_ONE.getIdentity()) {
            this.id = this.generateId(sender, receiver);
        }
    }

    private int chatType;
    private String id;


    public String key() {
        if (this.chatType < 0 || this.chatType >= BASE62_CHARS.length()) {
            throw new IllegalArgumentException("Session chat type must be between 0 and 61");
        }
        if (this.id == null || this.id.isEmpty()) {
            throw new IllegalArgumentException("Session ID must not be empty");
        }
        return BASE62_CHARS.charAt(this.chatType) + this.id;
    }

    public boolean isOne2One() {
        return this.chatType == ChatType.ONE_2_ONE.getIdentity();
    }

    private String generateId(ChatUser sender, ChatUser receiver) {
        validateOneToOneUser(sender);
        validateOneToOneUser(receiver);
        ChatUser bigUser = sender;
        ChatUser smallUser = receiver;
        int comparison = sender.getId().compareTo(receiver.getId());
        if (comparison == 0) {
            comparison = sender.getSessionCategory().compareTo(receiver.getSessionCategory());
        }
        if (comparison <= 0) {
            bigUser = receiver;
            smallUser = sender;
        }
        char bigCategory = BASE62_CHARS.charAt(bigUser.getSessionCategory());
        char smallCategory = BASE62_CHARS.charAt(smallUser.getSessionCategory());
        char length = BASE62_CHARS.charAt(bigUser.getId().length());
        return "" + bigCategory + smallCategory + length + bigUser.getId() + smallUser.getId();
    }

    static void validateOneToOneUser(ChatUser user) {
        if (user == null || user.getId() == null || user.getId().isEmpty() || user.getId().length() >= BASE62_CHARS.length()) {
            throw new IllegalArgumentException("Single-chat user ID must contain 1–61 characters");
        }
        if (user.getCategory() == null || user.getCategory() < 0 || user.getCategory() >= BASE62_CHARS.length()) {
            throw new IllegalArgumentException("Single-chat user category must be between 0 and 61");
        }
    }

    public static ChatSession parse(String sessionKey) {
        if (sessionKey == null || sessionKey.isEmpty()) {
            return null;
        }
        int chatType = decodeBase62(sessionKey.charAt(0));
        if (sessionKey.length() < 2) {
            throw new IllegalArgumentException("Session ID must not be empty");
        }
        String id = sessionKey.substring(1);
        return ChatSession.createSession(chatType, id);
    }

    public Pair<ChatUser, ChatUser> get1To1Members() {
        if (this.chatType != ChatType.ONE_2_ONE.getIdentity()) {
            return null;
        }

        if (this.id == null || this.id.length() < 5) {
            throw new IllegalArgumentException("Invalid single-chat session key");
        }
        int bigUserCategory = decodeBase62(this.id.charAt(0));
        int smallUserCategory = decodeBase62(this.id.charAt(1));
        int bigUserLength = decodeBase62(this.id.charAt(2));
        if (bigUserLength == 0 || 3 + bigUserLength >= this.id.length()) {
            throw new IllegalArgumentException("Invalid single-chat user ID length");
        }
        String bigUserId = this.id.substring(3, 3 + bigUserLength);
        String smallUserId = this.id.substring(3 + bigUserLength);

        ChatUser bigUser = ChatUser.stringUserId(bigUserId, bigUserCategory);
        ChatUser smallUser = ChatUser.stringUserId(smallUserId, smallUserCategory);
        validateOneToOneUser(bigUser);
        validateOneToOneUser(smallUser);
        return new Pair<>(bigUser, smallUser);
    }

    private static int decodeBase62(char value) {
        int decoded = BASE62_CHARS.indexOf(value);
        if (decoded < 0) {
            throw new IllegalArgumentException("Session fields must be Base62 characters");
        }
        return decoded;
    }

    public ChatUser getOppositeUser(ChatUser currentUser) {
        if (!this.isOne2One() || currentUser == null) {
            return null;
        }
        Pair<ChatUser, ChatUser> members = this.get1To1Members();
        if (members.getFirst().equals(currentUser)) {
            return members.getSecond();
        }
        if (members.getSecond().equals(currentUser)) {
            return members.getFirst();
        }
        return null;
    }

    public boolean isOne2OneMember(ChatUser user) {
        if (!this.isOne2One() || user == null) {
            return false;
        }
        Pair<ChatUser, ChatUser> members = this.get1To1Members();
        return members.getFirst().equals(user) || members.getSecond().equals(user);
    }

    public boolean isVisitor() {
        if (!this.isOne2One()) {
            return false;
        }
        Pair<ChatUser, ChatUser> members = this.get1To1Members();
        return members.getFirst().isVisitor() || members.getSecond().isVisitor();
    }
}
