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

import com.sparrow.protocol.constant.magic.Symbol;
import com.sparrow.protocol.enums.DefaultUserCategory;
import lombok.Data;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Data
public class ChatUser {


    public static ChatUser parse(String key) {
        if (key == null) {
            return null;
        }
        int separator = key.lastIndexOf(Symbol.UNDERLINE);
        if (separator <= 0 || separator == key.length() - 1) {
            return null;
        }
        String id = key.substring(0, separator);
        Integer role = Integer.parseInt(key.substring(separator + 1));
        return new ChatUser(id, role);
    }

    public static ChatUser longUserId(Long id, Integer role) {
        if (id == null) {
            throw new IllegalArgumentException("User ID must not be null");
        }
        return new ChatUser(id.toString(), role);
    }

    public static ChatUser stringUserId(String id, Integer role) {
        return new ChatUser(id, role);
    }

    private ChatUser(String id, Integer role) {
        this.id = id;
        this.category = role;
    }

    private String id;
    private Integer category;

    public Integer getSessionCategory() {
        if (this.isVisitor()) {
            return DefaultUserCategory.VISITOR.getIdentity();
        }
        return category;
    }

    public Boolean isVisitor() {
        return Objects.equals(this.category, DefaultUserCategory.VISITOR.getIdentity());
    }

    public Boolean isAI() {
        return Objects.equals(this.category, DefaultUserCategory.AI.getIdentity());
    }

    public String key() {
        requireIdentity();
        return this.id + Symbol.UNDERLINE + this.getSessionCategory();
    }

    public boolean equals(ChatUser chatUser) {
        return equals((Object) chatUser);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ChatUser user && Objects.equals(this.id, user.id)
                && Objects.equals(this.category, user.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, category);
    }


    public Long getLongUserId() {
        return Long.parseLong(this.id);
    }

    /**
     * 用户ID长度(1字节) + 用户ID([用户ID长度个字节]) + 用户类型(1字节)
     *
     * @return
     */
    public byte[] toBytes() {
        requireIdentity();
        if (this.id.length() > 255 || this.category < 0 || this.category > 255) {
            throw new IllegalArgumentException("User ID must be 1–255 UTF-8 bytes and category must be between 0 and 255");
        }
        byte[] idBytes = this.id.getBytes(StandardCharsets.UTF_8);
        if (idBytes.length > 255) {
            throw new IllegalArgumentException("User ID must not exceed 255 UTF-8 bytes");
        }
        int capacity = 1 + idBytes.length + 1;
        byte[] byteBuf = new byte[capacity];
        byteBuf[0] = (byte) idBytes.length;
        System.arraycopy(idBytes, 0, byteBuf, 1, idBytes.length);
        byteBuf[capacity - 1] = this.category.byteValue();
        return byteBuf;
    }

    private void requireIdentity() {
        if (this.id == null || this.id.isEmpty() || this.category == null) {
            throw new IllegalArgumentException("User ID and category are required");
        }
    }
}
