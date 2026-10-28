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

import com.sparrow.messaging.protocol.ProtocolSegmentIndex;
import com.sparrow.messaging.protocol.ProtocolType;
import lombok.Getter;

@Getter
public enum DefaultProtocolType implements ProtocolType {
    AI_REQUEST(0, ProtocolSegmentIndex.USER_SEGMENT, TransmissionDirection.UPLINK, true, true),
    AI_RESPONSE(1, ProtocolSegmentIndex.USER_SEGMENT, TransmissionDirection.DOWNLINK, true, true),

    IM(8, ProtocolSegmentIndex.USER_SEGMENT, TransmissionDirection.BIDIRECTIONAL, true, true),
    //服务器收到消息
    ACK(10, ProtocolSegmentIndex.TIME_SEGMENT, TransmissionDirection.BIDIRECTIONAL, false, true),
    //消息已受理
    MESSAGE_ACCEPT(25, ProtocolSegmentIndex.TIME_SEGMENT, TransmissionDirection.DOWNLINK, false, true),
    //撤消消息
    MESSAGE_CANCEL(27, ProtocolSegmentIndex.TIME_SEGMENT, TransmissionDirection.DOWNLINK, false, true),
    //消息已处理
    MESSAGE_DEAL(28, ProtocolSegmentIndex.TIME_SEGMENT, TransmissionDirection.DOWNLINK, false, true),
    //用户已读
    READ(20, ProtocolSegmentIndex.TIME_SEGMENT, TransmissionDirection.UPLINK, false, true),
    //因为浏览器不一定支持PING PONG websocket 协议，需要自定义实现
    PING(30, ProtocolSegmentIndex.PROTOCOL_SEGMENT, TransmissionDirection.UPLINK, false, true),
    PONG(40, ProtocolSegmentIndex.PROTOCOL_SEGMENT, TransmissionDirection.DOWNLINK, false, true),
    FETCH_USER_STATUS(45, ProtocolSegmentIndex.PROTOCOL_SEGMENT, TransmissionDirection.UPLINK, false, true),
    //监控联系人状态
    SEND_USER_STATUS(50, ProtocolSegmentIndex.CONTENT_SEGMENT, TransmissionDirection.DOWNLINK, false, true),
    //对方不在线
    OFFLINE(70, ProtocolSegmentIndex.PROTOCOL_SEGMENT, TransmissionDirection.DOWNLINK, false, true),
    //会话已结束，推送另一方
    ENDED_CONVERSATION(80, ProtocolSegmentIndex.CONTENT_SEGMENT, TransmissionDirection.DOWNLINK, false, true),
    //异常
    ERROR(-99, ProtocolSegmentIndex.PROTOCOL_SEGMENT, TransmissionDirection.DOWNLINK, false, true);

    private final int id;
    private final Integer endOfSegmentIndex;
    private final TransmissionDirection direction;
    private final Boolean localImmediately;
    private final Boolean needBusiness;

    DefaultProtocolType(Integer id, Integer endOfSegmentIndex, TransmissionDirection direction, Boolean needBusiness, Boolean localImmediately) {
        this.id = id;
        this.endOfSegmentIndex = endOfSegmentIndex;
        this.direction = direction;
        this.needBusiness = needBusiness;
        this.localImmediately = localImmediately;
    }

    @Override
    public Integer getIdentity() {
        return this.id;
    }


    @Override
    public String getDisplayName() {
        return this.name();
    }


    public static ProtocolType valueOf(Integer identity) {
        for (ProtocolType eventType : values()) {
            if (eventType.getIdentity().equals(identity)) {
                return eventType;
            }
        }
        return null;
    }

    @Override
    public TransmissionDirection direction() {
        return this.direction;
    }

    @Override
    public Boolean needBusiness() {
        return this.needBusiness;
    }

    @Override
    public Boolean localImmediately() {
        return this.localImmediately;
    }
}
