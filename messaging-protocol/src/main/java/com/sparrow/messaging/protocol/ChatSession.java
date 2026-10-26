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
 * 沿用既有 key 格式：类别各占一位，较大用户 ID 的字符长度占两位。
 * 因此单聊类别限 0–9，双方 ID 的 String.length() 限 1–99；超出时明确拒绝，不截断。
 */
@Data
@NoArgsConstructor
public class ChatSession {


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
        return chatType + id;
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
        int length = bigUser.getId().length();
        String len = String.valueOf(length);
        if (len.length() == 1) {
            len = "0" + len;
        }
        return bigUser.getSessionCategory() + "" + smallUser.getSessionCategory() + "" + len + "" + bigUser.getId() + smallUser.getId();
    }

    static void validateOneToOneUser(ChatUser user) {
        if (user == null || user.getId() == null || user.getId().isEmpty() || user.getId().length() > 99) {
            throw new IllegalArgumentException("Single-chat user ID must contain 1–99 characters");
        }
        if (user.getCategory() == null || user.getCategory() < 0 || user.getCategory() > 9) {
            throw new IllegalArgumentException("Single-chat user category must be between 0 and 9");
        }
    }

    public static ChatSession parse(String sessionKey) {
        if (sessionKey == null || sessionKey.isEmpty()) {
            return null;
        }
        int chatType = Integer.parseInt(sessionKey.substring(0, 1));
        String id = sessionKey.substring(1);
        return ChatSession.createSession(chatType, id);
    }

    public Pair<ChatUser, ChatUser> get1To1Members() {
        if (this.chatType != ChatType.ONE_2_ONE.getIdentity()) {
            return null;
        }

        if (this.id == null || this.id.length() < 6) {
            throw new IllegalArgumentException("Invalid single-chat session key");
        }
        for (int i = 0; i < 4; i++) {
            if (this.id.charAt(i) < '0' || this.id.charAt(i) > '9') {
                throw new IllegalArgumentException("Single-chat category and ID length must be decimal digits");
            }
        }
        String bigUserCategory = this.id.substring(0, 1);
        String smallUserCategory = this.id.substring(1, 2);
        int bitUserLength = Integer.parseInt(this.id.substring(2, 4));
        if (bitUserLength == 0 || 4 + bitUserLength >= this.id.length()) {
            throw new IllegalArgumentException("Invalid single-chat user ID length");
        }
        String bigUserId = this.id.substring(4, 4 + bitUserLength);
        String smallUserId = this.id.substring(4 + bitUserLength);

        ChatUser bigUser = ChatUser.stringUserId(bigUserId, Integer.valueOf(bigUserCategory));
        ChatUser smallUser = ChatUser.stringUserId(smallUserId, Integer.valueOf(smallUserCategory));
        validateOneToOneUser(bigUser);
        validateOneToOneUser(smallUser);
        return new Pair<>(bigUser, smallUser);
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
