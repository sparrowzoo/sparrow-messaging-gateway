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

public class MessageUniqueIdentify {
    /**
     * 兼容时间戳作为消息 ID 的调用方式；调用方必须保证同用户的 clientSendTime 不重复。
     * 多设备或同毫秒多条消息应使用带 clientMessageId 的构造方法。
     */
    public MessageUniqueIdentify(ChatUser fromUser, Long clientSendTime) {
        this(fromUser, clientSendTime, null);
    }

    /**
     * clientMessageId 由客户端为每条消息赋值；重试须复用原时间和 ID，以保持幂等键稳定。
     * null 保留两参数的时间戳模式；非 null 时不能为空字符串。
     */
    public MessageUniqueIdentify(ChatUser fromUser, Long clientSendTime, String clientMessageId) {
        if (fromUser == null || fromUser.getId() == null || fromUser.getId().isEmpty()
                || fromUser.getCategory() == null || clientSendTime == null || clientSendTime < 0) {
            throw new IllegalArgumentException("Message identity requires a user and a non-negative clientSendTime");
        }
        if (clientMessageId != null && clientMessageId.isEmpty()) {
            throw new IllegalArgumentException("clientMessageId must not be empty");
        }
        this.fromUserId = fromUser.getId();
        this.userCategory = fromUser.getCategory();
        this.clientSendTime = clientSendTime;
        this.clientMessageId = clientMessageId;
    }

    private final String fromUserId;
    private final Integer userCategory;
    private final Long clientSendTime;
    private final String clientMessageId;

    public String key() {
        // 长度前缀限定字符串字段的边界，即使 ID 自带分隔符也不会串字段。
        String userIdentity = this.userCategory + "-" + this.fromUserId.length() + ":" + this.fromUserId;
        if (this.clientMessageId == null) {
            return this.clientSendTime + "-" + userIdentity;
        }
        return "message-" + this.clientSendTime + "-" + userIdentity
                + "-" + this.clientMessageId.length() + ":" + this.clientMessageId;
    }
}
