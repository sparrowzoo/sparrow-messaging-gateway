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

package com.sparrow.messaging.domain.service;

import com.sparrow.protocol.LoginUser;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

/**
 * 与传输框架无关的单个物理连接。身份访问返回握手捕获值，不从当前线程或客户端重新解析。
 */
public interface ClientConnection {

    LoginUser loginUser();

    /**
     * 接收有界、不可变的完整编码帧快照；准入后在返回前启动实际非阻塞 write，不能仅应用入队。
     * 支持并发发送；CompletionStage 完成只证明本地写完成，不代表客户端 ACK。
     * 关闭或不可写时异常完成。
     * 不在业务锁内等待返回阶段。
     */
    CompletionStage<Void> sendMessage(ByteBuffer encodedFrame);

    /**
     * 使用合法 WebSocket 关闭码及脱敏原因发起幂等关闭；不承诺对端已收到关闭帧。
     */
    void closeConnection();

    /**
     * 获取最后访问时间
     *
     * @return
     */
    Long getLastAccessTime();

    void refreshAccessTime();

    Long getCreateTime();

    Long getDuration();
}
