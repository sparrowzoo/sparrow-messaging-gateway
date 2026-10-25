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

import com.sparrow.messaging.protocol.ChatUser;
import com.sparrow.protocol.BusinessException;
import com.sparrow.protocol.LoginUser;
import com.sparrow.protocol.enums.DeviceType;

import java.util.Map;

/**
 * 用户连接目录：{@code Map<Long, Map<Long, ClientConnection>>}，外层键为可信用户 ID。
 * 内层 connectionId 暂用设备类型的数值编码，后续可扩展；不以 session/conversation 登记或查询。
 * 同用户同设备只允许一个有效连接；新连接认证成功后踢掉并替换旧连接，旧回调不得删除新连接。
 */
public interface ConnectionRegistry {

    /**
     * 握手认证成功即可登记；用户不需要已有 session 或咨询。身份与设备来自 connection 的可信附件。
     */
    ClientConnection registerConnection(ClientConnection connection) throws BusinessException;

    /**
     * 精确查询用户下一个连接；不存在返回 null，不进行咨询授权或创建咨询。
     */
    ClientConnection findConnection(LoginUser loginUser) throws BusinessException;

    ClientConnection findConnection(ChatUser chatUser,DeviceType deviceType);

    /**
     * 用户下全部有效连接的只读快照；没有连接返回空列表。调用方负责消息的业务授权。
     */
    Map<DeviceType, ClientConnection> findConnections(Long userId) throws BusinessException;

    Map<DeviceType, ClientConnection> findConnections(LoginUser loginUser) throws BusinessException;

    /**
     * 仅受控关闭回调使用；仅当目录仍指向 expectedConnection 时删除，防止旧连接回调删掉同设备的新连接。
     */
    ClientConnection removeConnection(LoginUser loginUser);

    Map<DeviceType, ClientConnection> findConnections(ChatUser chatUser);

}
