/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sparrow.messaging.protocol.enums;

import com.sparrow.protocol.EnumIdentityAccessor;

public enum ChatType implements EnumIdentityAccessor {

    ONE_2_ONE(0),

    GROUP(1);

    ChatType(int id) {
        this.id = id;
    }

    private final int id;

    @Override
    public Integer getIdentity() {
        return this.id;
    }

    public static Boolean isOne2One(ChatType chatType) {
        return ChatType.ONE_2_ONE.equals(chatType);
    }

    public static Boolean isOne2One(int chatType) {
        return ChatType.ONE_2_ONE.getIdentity().equals(chatType);
    }

    public static Boolean isGroup(ChatType chatType) {
        return ChatType.GROUP.equals(chatType);
    }

    public static Boolean isGroup(int chatType) {
        return ChatType.GROUP.getIdentity().equals(chatType);
    }

    public static ChatType valueOf(int identity) {
        for (ChatType chatType : values()) {
            if (chatType.getIdentity() == identity) {
                return chatType;
            }
        }
        return null;
    }
}
