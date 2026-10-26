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

import com.sparrow.messaging.protocol.EventType;
import com.sparrow.messaging.protocol.ProtocolSegmentIndex;
import lombok.Getter;


@Getter
public enum DefaultEventType implements EventType {
    AI(0, ProtocolSegmentIndex.USER_SEGMENT, true),
    IM(8, ProtocolSegmentIndex.USER_SEGMENT, true),
    //服务器收到消息
    ACK(10, ProtocolSegmentIndex.TIME_SEGMENT, false),
    //消息已受理
    MESSAGE_ACCEPT(25, ProtocolSegmentIndex.TIME_SEGMENT, false),
    //撤消消息
    MESSAGE_CANCEL(27, ProtocolSegmentIndex.TIME_SEGMENT, false),
    //消息已处理
    MESSAGE_DEAL(28, ProtocolSegmentIndex.TIME_SEGMENT, false),
    //用户已读
    READ(20, ProtocolSegmentIndex.TIME_SEGMENT, false),
    //因为浏览器不一定支持PING PONG websocket 协议，需要自定义实现
    PING(30, ProtocolSegmentIndex.EVENT_SEGMENT, false),
    PONG(40, ProtocolSegmentIndex.EVENT_SEGMENT, false),
    //监控联系人状态
    USER_STATUS(50, ProtocolSegmentIndex.CONTENT_SEGMENT, false),
    //对方不在线
    OFFLINE(70, ProtocolSegmentIndex.EVENT_SEGMENT, false),
    //会话已结束，推送另一方
    ENDED_CONVERSATION(80, ProtocolSegmentIndex.CONTENT_SEGMENT, false),
    //异常
    ERROR(-99, ProtocolSegmentIndex.EVENT_SEGMENT, false);

    private final int id;
    private final Integer endOfSegmentIndex;
    private final Boolean publishable;

    DefaultEventType(Integer id, Integer endOfSegmentIndex, Boolean publishable) {
        this.id = id;
        this.endOfSegmentIndex = endOfSegmentIndex;
        this.publishable = publishable;
    }

    @Override
    public Integer getIdentity() {
        return this.id;
    }


    public static EventType valueOf(Integer identity) {
        for (EventType eventType : values()) {
            if (eventType.getIdentity().equals(identity)) {
                return eventType;
            }
        }
        return null;
    }


    @Override
    public Boolean publishable() {
        return publishable;
    }
}
