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
import com.sparrow.messaging.protocol.enums.DefaultEventType;
import com.sparrow.protocol.enums.DeviceType;
import lombok.Data;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;


/**
 * 使用 JDK 完成 IM 报文编解码，不依赖网络框架。
 *
 * <pre>
 * 事件段：eventType(1)
 * 控制及内容段：device(1) + chatType(1) + messageType(2)
 *              + payload 的 UTF-8 字节数(4) + payload
 * 时间段：时间文本的字节数(1) + clientSendTime[_serverTime]
 * 用户会话段：sender + receiver（单聊）
 *          或 sender + sessionKey 的 UTF-8 字节数(1) + sessionKey（群聊）
 * 用户结构：ID 的字节数(1) + ID + category(1)
 * </pre>
 * eventType 决定报文结束于哪一段；PONG 等仅包含事件段的报文占一个字节。
 * 所有文本使用 UTF-8，多字节数值使用大端；长度和用户类别按无符号数处理。
 * messageType 用 short 保留完整的 16 位，业务上可使用 Short.toUnsignedInt 获取无符号值。
 * 单聊为兼容既有会话 key，双方类别限 0–9，ID 的 String.length() 限 1–99；
 * 同时仍受用户 ID 最多 255 个 UTF-8 字节的线上限制。群聊沿用原用户结构和 sessionKey 限制。
 * 调用方必须提供完整的一帧：WebSocket 分片需先聚合，TCP 字节流需在外层划分消息边界。
 *
 * <p>Netty 接入示例（仅调用方需要引入 Netty）：
 * <pre>{@code
 * // 接收：只读取可读区，不改变 ByteBuf 的 readerIndex / writerIndex。
 * Protocol protocol = new Protocol(
 *     content.nioBuffer(content.readerIndex(), content.readableBytes()));
 *
 * // 发送：服务端明确填写时间；客户端发送时 serverTime 保持 null。
 * protocol.setServerTime(System.currentTimeMillis());
 * BinaryWebSocketFrame frame = new BinaryWebSocketFrame(
 *     Unpooled.wrappedBuffer(protocol.encode()));
 * channel.writeAndFlush(frame);
 * }</pre>
 * 原 ByteBuf 构造方法改为传入 NIO 视图，原 encode(allocator, capacity) 改为包装 encode() 的结果。
 * 编码已包含填写的服务器时间，不要再追加一次时间后缀。
 * 复合 ByteBuf 转 NIO 视图可能发生复制；不支持 NIO 视图时，可在调用方用 getBytes 复制可读区。
 * 本类同步解析且不持有输入缓冲区，ByteBuf 的引用计数与释放仍由 Netty 调用方管理。
 */
@Data
public class Protocol implements TypedEvent {
    public Protocol() {
    }

    private DeviceType device;
    /**
     * 业务消息类型 ID，范围 0–65535；在线协议中占两个字节，决定正文解析格式。
     */
    private short messageType;
    /**
     * @see ChatType
     */
    private ChatType chatType;
    /**
     * @see com.sparrow.messaging.protocol.enums.DefaultEventType
     */
    private EventType eventType;
    /**
     * 群聊 sessionKey 的 UTF-8 字节数；编解码时根据实际内容计算。
     */
    private int sessionLength;
    /**
     * 用户会话段的发送者；结束于此前各段的报文不包含该字段。
     */
    private ChatUser sender;
    /**
     * 接收者信息
     */
    private ChatUser receiver;
    /**
     * 会话
     */
    private ChatSession chatSession;
    /**
     * 消息内容；null 或空字符串编码为零长度载荷，解码零长度载荷时为 null。
     * 业务事件是否需要正文由调用方校验，编解码不补默认内容。
     */
    private String payload;
    private Long clientSendTime;
    /**
     * null 表示时间段只包含客户端时间；非空时追加“_服务器时间戳”。
     * 编码不会自动刷新时间，重复编码同一消息可保持相同字节。
     */
    private Long serverTime;

    /**
     * 解析数组中的完整一帧，不修改或保留输入数组。
     *
     * @param frame 完整报文
     * @throws IllegalArgumentException 报文被截断或字段无效
     */
    public Protocol(byte[] frame) {
        // "frame" 是参数为 null 时的异常提示，不是默认消息；wrap 不会补入任何内容。
        this(ByteBuffer.wrap(Objects.requireNonNull(frame, "frame")));
    }

    /**
     * 解析 position 到 limit 之间的完整一帧。
     * 支持堆内存、直接内存、只读和切片缓冲区；成功或失败均不改变输入的位置、标记和字节序。
     *
     * @param frame 有效范围恰好包含一条消息的缓冲区
     * @throws IllegalArgumentException 报文被截断或字段无效
     */
    public Protocol(ByteBuffer frame) {
        ByteBuffer source = Objects.requireNonNull(frame, "frame").slice().order(ByteOrder.BIG_ENDIAN);
        requireRemaining(source, 1, "eventType");
        this.eventType = DefaultEventType.valueOf((int) source.get());
        if (this.eventType == null) {
            throw new IllegalArgumentException("event type is not found");
        }
        //只解析第一段
        if (this.eventType.getEndOfSegmentIndex() == ProtocolSegmentIndex.EVENT_SEGMENT) {
            return;
        }
        // 控制段：设备、聊天类型各一字节，消息类型两字节。
        requireRemaining(source, 4, "header");
        this.device = DeviceType.getDeviceById((int) source.get());
        if (this.device == null) {
            throw new IllegalArgumentException("device is not found");
        }
        this.chatType = ChatType.valueOf(source.get());
        if (this.chatType == null) {
            throw new IllegalArgumentException("chat type is not found");
        }
        this.messageType = source.getShort();

        requireRemaining(source, 4, "payloadLength");
        int payloadLength = source.getInt();
        if (payloadLength < 0) {
            throw new IllegalArgumentException("payloadLength must not be negative");
        }
        if (payloadLength > 0) {
            this.payload = new String(readBytes(source, payloadLength, "payload"), StandardCharsets.UTF_8);
        }
        //只到内容段
        if (this.eventType.getEndOfSegmentIndex() == ProtocolSegmentIndex.CONTENT_SEGMENT) {
            return;
        }

        //时间段长度
        requireRemaining(source, 1, "timeLength");
        int timeLength = Byte.toUnsignedInt(source.get());
        String timestamps = new String(readBytes(source, timeLength, "timestamps"), StandardCharsets.UTF_8);
        if (!timestamps.matches("[0-9]+(?:_[0-9]+)?")) {
            throw new IllegalArgumentException("timestamps must contain one non-negative decimal integer or two separated by '_'");
        }
        String[] timestampArray = timestamps.split("_", -1);
        this.clientSendTime = Long.valueOf(timestampArray[0]);
        if (timestampArray.length == 2) {
            this.serverTime = Long.valueOf(timestampArray[1]);
        }

        if (this.eventType.getEndOfSegmentIndex() == ProtocolSegmentIndex.TIME_SEGMENT) {
            return;
        }
        //用户会话段
        this.sender = parseUser(source);
        if (this.isOne2One()) {
            this.receiver = parseUser(source);
            this.chatSession = ChatSession.create1To1Session(this.sender, this.receiver);
            return;
        }
        requireRemaining(source, 1, "sessionLength");
        this.sessionLength = Byte.toUnsignedInt(source.get());
        String sessionKey = new String(readBytes(source, this.sessionLength, "sessionKey"), StandardCharsets.UTF_8);
        if (sessionKey.length() < 2 || sessionKey.charAt(0) != '0' + this.chatType.getIdentity()) {
            throw new IllegalArgumentException("sessionKey must contain the chat type prefix and a non-empty ID");
        }
        this.chatSession = ChatSession.parse(sessionKey);
    }

    /**
     * 读取一个用户，成功后推进 source 的位置；完整报文构造方法会在独立视图上调用它。
     *
     * @param source 当前位于用户结构开头的缓冲区
     * @return 用户 ID 和类别
     */
    public ChatUser parseUser(ByteBuffer source) {
        requireRemaining(source, 1, "user.idLength");
        int idLength = Byte.toUnsignedInt(source.get());
        if (idLength == 0) {
            throw new IllegalArgumentException("User ID must not be empty");
        }
        requireRemaining(source, idLength + 1, "user");
        String id = new String(readBytes(source, idLength, "user.id"), StandardCharsets.UTF_8);
        return ChatUser.stringUserId(id, Byte.toUnsignedInt(source.get()));
    }


    public boolean isOne2One() {
        return ChatType.isOne2One(this.chatType);
    }

    public void setChatType(ChatType chatType) {
        this.chatType = chatType;
        refreshOneToOneSession();
    }

    public void setSender(ChatUser sender) {
        this.sender = sender;
        refreshOneToOneSession();
    }

    public void setReceiver(ChatUser receiver) {
        this.receiver = receiver;
        refreshOneToOneSession();
    }

    private void refreshOneToOneSession() {
        if (this.isOne2One()) {
            this.chatSession = this.sender == null || this.receiver == null ? null
                    : ChatSession.create1To1Session(this.sender, this.receiver);
        }
    }

    /**
     * 按 eventType 指定的结束段编码，字段顺序与完整报文构造方法一致。
     * 事件段之后的字段仅在对应段需要时校验；payload 可为空。
     * 包含时间段时 clientSendTime 必填，serverTime 可为空；包含用户会话段时用户和目标必填。
     *
     * @return 可直接读取的完整报文，position 为 0，limit 为报文长度，无需调用 flip()
     * @throws IllegalArgumentException 必填字段缺失、长度超限或字段值无效
     */
    public ByteBuffer encode() {
        if (this.eventType == null) {
            throw new IllegalArgumentException("eventType is required");
        }
        EventType wireEventType = DefaultEventType.valueOf(this.eventType.getIdentity());
        if (wireEventType == null) {
            throw new IllegalArgumentException("event type is not found");
        }
        int endOfSegmentIndex = wireEventType.getEndOfSegmentIndex();
        if (endOfSegmentIndex == ProtocolSegmentIndex.EVENT_SEGMENT) {
            return ByteBuffer.wrap(new byte[]{wireEventType.getIdentity().byteValue()});
        }
        if (this.device == null || this.chatType == null) {
            throw new IllegalArgumentException("device and chatType are required for the control segment");
        }
        byte[] payloadBytes = this.payload == null ? new byte[0] : this.payload.getBytes(StandardCharsets.UTF_8);
        //预留协议长度为后续段扩展
        int capacity = payloadBytes.length * 2;
        ByteArrayOutputStream result = new ByteArrayOutputStream(capacity);
        //一个字节
        result.write(wireEventType.getIdentity());
        result.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN)
                .put(this.device.getIdentity().byteValue())
                .put(this.chatType.getIdentity().byteValue())
                .putShort(this.messageType).array());
        result.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(payloadBytes.length).array());
        result.writeBytes(payloadBytes);
        if (endOfSegmentIndex == ProtocolSegmentIndex.CONTENT_SEGMENT) {
            return ByteBuffer.wrap(result.toByteArray());
        }

        if (this.clientSendTime == null) {
            throw new IllegalArgumentException("clientSendTime is required for the time segment");
        }
        if (this.clientSendTime < 0 || (this.serverTime != null && this.serverTime < 0)) {
            throw new IllegalArgumentException("clientSendTime and serverTime must not be negative");
        }
        String timestamps = this.clientSendTime.toString();
        if (this.serverTime != null) {
            timestamps += "_" + this.serverTime;
        }
        byte[] timeBytes = timestamps.getBytes(StandardCharsets.UTF_8);
        result.write(timeBytes.length);
        result.writeBytes(timeBytes);
        if (endOfSegmentIndex == ProtocolSegmentIndex.TIME_SEGMENT) {
            return ByteBuffer.wrap(result.toByteArray());
        }

        byte[] senderBytes = encodeUser(this.sender, "sender");
        if (this.isOne2One()) {
            ChatSession.validateOneToOneUser(this.sender);
            ChatSession.validateOneToOneUser(this.receiver);
            byte[] receiverBytes = encodeUser(this.receiver, "receiver");
            this.sessionLength = 0;
            result.writeBytes(senderBytes);
            result.writeBytes(receiverBytes);
            return ByteBuffer.wrap(result.toByteArray());
        }
        if (this.chatSession == null || this.chatSession.getChatType() != this.chatType.getIdentity()
                || this.chatSession.getId() == null || this.chatSession.getId().isEmpty()) {
            throw new IllegalArgumentException("Group chat requires a matching session with a non-empty ID");
        }
        byte[] sessionBytes = this.chatSession.key().getBytes(StandardCharsets.UTF_8);
        if (sessionBytes.length > 255) {
            throw new IllegalArgumentException("sessionKey UTF-8 byte length must not exceed 255");
        }
        this.sessionLength = sessionBytes.length;
        result.writeBytes(senderBytes);
        result.write(sessionBytes.length);
        result.writeBytes(sessionBytes);
        return ByteBuffer.wrap(result.toByteArray());
    }

    private static byte[] encodeUser(ChatUser user, String field) {
        if (user == null || user.getId() == null || user.getId().isEmpty()) {
            throw new IllegalArgumentException(field + " must have a non-empty ID");
        }
        if (user.getId().length() > 255) {
            throw new IllegalArgumentException(field + ".id UTF-8 byte length must not exceed 255");
        }
        byte[] idBytes = user.getId().getBytes(StandardCharsets.UTF_8);
        if (idBytes.length > 255) {
            throw new IllegalArgumentException(field + ".id UTF-8 byte length must not exceed 255");
        }
        if (user.getCategory() == null || user.getCategory() < 0 || user.getCategory() > 255) {
            throw new IllegalArgumentException(field + ".category must be between 0 and 255");
        }
        // 用户结构在协议层统一写入，不依赖 ChatUser.toBytes() 的实现。
        return ByteBuffer.allocate(idBytes.length + 2).put((byte) idBytes.length)
                .put(idBytes).put(user.getCategory().byteValue()).array();
    }


    private static byte[] readBytes(ByteBuffer source, int length, String field) {
        requireRemaining(source, length, field);
        byte[] bytes = new byte[length];
        source.get(bytes);
        return bytes;
    }

    private static void requireRemaining(ByteBuffer source, int length, String field) {
        if (length < 0 || length > source.remaining()) {
            throw new IllegalArgumentException("Invalid " + field + " length: " + length
                    + ", remaining bytes: " + source.remaining());
        }
    }


    /**
     * 生成仅包含事件类型的 PONG，不修改原请求。
     */
    public static Protocol pong() {
        Protocol pong = new Protocol();
        pong.eventType = DefaultEventType.PONG;
        return pong;
    }

    public Protocol ack() {
        Protocol ack = clone();
        ack.serverTime = System.currentTimeMillis();
        ack.eventType = DefaultEventType.ACK;
        return ack;
    }

    /**
     * 浅拷贝所有字段；发送者、接收者和会话对象与原消息共享。
     */
    public Protocol clone() {
        Protocol clone = new Protocol();
        clone.device = this.device;
        clone.messageType = this.messageType;
        clone.chatType = this.chatType;
        clone.eventType = this.eventType;
        clone.sessionLength = this.sessionLength;
        clone.sender = this.sender;
        clone.receiver = this.receiver;
        clone.chatSession = this.chatSession;
        clone.payload = this.payload;
        clone.clientSendTime = this.clientSendTime;
        clone.serverTime = this.serverTime;
        return clone;
    }
}
