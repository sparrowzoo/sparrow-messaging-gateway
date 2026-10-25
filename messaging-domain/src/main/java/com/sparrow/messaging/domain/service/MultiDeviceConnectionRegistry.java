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

import java.util.HashMap;
import java.util.Map;

public class MultiDeviceConnectionRegistry implements ConnectionRegistry {
    private Map<String, Map<DeviceType, ClientConnection>> registry = new HashMap<>();

    @Override
    public ClientConnection registerConnection(ClientConnection connection) {
        LoginUser loginUser = connection.loginUser();
        ChatUser chatUser = ChatUser.longUserId(loginUser.getUserId(), loginUser.getCategory());
        registry.computeIfAbsent(chatUser.key(), k -> new HashMap<>());
        ClientConnection oldConnection = registry.get(chatUser.key()).get(loginUser.getDeviceType());
        registry.get(chatUser.key()).put(loginUser.getDeviceType(), connection);
        return oldConnection;
    }

    @Override
    public ClientConnection findConnection(LoginUser loginUser) throws BusinessException {
        ChatUser chatUser = ChatUser.longUserId(loginUser.getUserId(), loginUser.getCategory());
        Map<DeviceType, ClientConnection> connectionMap = registry.get(chatUser.key());
        if (connectionMap != null) {
            return connectionMap.get(loginUser.getDeviceType());
        }
        return null;
    }

    @Override
    public ClientConnection findConnection(ChatUser chatUser, DeviceType deviceType) {
        Map<DeviceType, ClientConnection> connectionMap = registry.get(chatUser.key());
        if (connectionMap != null) {
            return connectionMap.get(deviceType);
        }
        return null;
    }

    @Override
    public Map<DeviceType, ClientConnection> findConnections(LoginUser loginUser) throws BusinessException {
        ChatUser chatUser = ChatUser.longUserId(loginUser.getUserId(), loginUser.getCategory());
        return registry.get(chatUser.key());
    }

    @Override
    public Map<DeviceType, ClientConnection> findConnections(ChatUser chatUser) {
        return registry.get(chatUser.key());
    }

    public Map<DeviceType, ClientConnection> findConnections(Long userId) throws BusinessException {
        ChatUser chatUser = ChatUser.longUserId(userId, LoginUser.CATEGORY_REGISTER);
        return registry.get(chatUser.key());
    }


    @Override
    public ClientConnection removeConnection(LoginUser query) {
        ChatUser chatUser = ChatUser.longUserId(query.getUserId(), query.getCategory());
        Map<DeviceType, ClientConnection> connectionMap = registry.get(chatUser.key());
        if (connectionMap != null) {
            ClientConnection connection = connectionMap.remove(query.getDeviceType());
            connection.closeConnection();
            return connection;
        }
        return null;
    }
}
