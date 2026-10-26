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
import java.util.Objects;

public class MultiDeviceConnectionRegistry implements ConnectionRegistry {
    private final Object lock = new Object();
    private final Map<String, Map<DeviceType, ClientConnection>> registry = new HashMap<>();

    @Override
    public ClientConnection register(ClientConnection connection) {
        LoginUser user = connection.loginUser();
        String key = ChatUser.longUserId(user.getUserId(), user.getCategory()).key();
        synchronized (lock) {
            Map<DeviceType, ClientConnection> connections = registry.computeIfAbsent(key, k -> new HashMap<>());
            ClientConnection previous = connections.put(user.getDeviceType(), connection);
            return previous == connection ? null : previous;
        }
    }

    @Override
    public ClientConnection findConnection(LoginUser user) throws BusinessException {
        return findConnection(ChatUser.longUserId(user.getUserId(), user.getCategory()), user.getDeviceType());
    }

    @Override
    public ClientConnection findConnection(ChatUser user, DeviceType device) {
        synchronized (lock) {
            Map<DeviceType, ClientConnection> connections = registry.get(user.key());
            return connections == null ? null : connections.get(device);
        }
    }

    @Override
    public Map<DeviceType, ClientConnection> findConnections(LoginUser user) throws BusinessException {
        return findConnections(ChatUser.longUserId(user.getUserId(), user.getCategory()));
    }

    @Override
    public Map<DeviceType, ClientConnection> findConnections(ChatUser user) {
        synchronized (lock) {
            Map<DeviceType, ClientConnection> connections = registry.get(user.key());
            return connections == null ? Map.of() : Map.copyOf(connections);
        }
    }

    @Override
    public ClientConnection getRecentlyConnection(ChatUser user) {
        ClientConnection recent = null;
        long lastAccess = Long.MIN_VALUE;
        for (ClientConnection current : findConnections(user).values()) {
            Long access = current.getLastAccessTime();
            long timestamp = access == null ? Long.MIN_VALUE : access;
            if (recent == null || timestamp > lastAccess) {
                recent = current;
                lastAccess = timestamp;
            }
        }
        return recent;
    }

    @Override
    public Map<String, Map<DeviceType, ClientConnection>> getAll() {
        synchronized (lock) {
            Map<String, Map<DeviceType, ClientConnection>> snapshot = new HashMap<>();
            registry.forEach((user, connections) -> snapshot.put(user, Map.copyOf(connections)));
            return Map.copyOf(snapshot);
        }
    }

    @Override
    public Map<DeviceType, ClientConnection> findConnections(Long userId) throws BusinessException {
        return findConnections(ChatUser.longUserId(userId, LoginUser.CATEGORY_REGISTER));
    }

    @Override
    public ClientConnection removeConnection(LoginUser user) {
        return remove(user, null);
    }

    @Override
    public ClientConnection removeConnection(LoginUser user, ClientConnection expectedConnection) {
        Objects.requireNonNull(expectedConnection, "expectedConnection");
        return remove(user, expectedConnection);
    }

    private ClientConnection remove(LoginUser user, ClientConnection expectedConnection) {
        String key = ChatUser.longUserId(user.getUserId(), user.getCategory()).key();
        ClientConnection removed;
        synchronized (lock) {
            Map<DeviceType, ClientConnection> connections = registry.get(key);
            if (connections == null) {
                return null;
            }
            ClientConnection current = connections.get(user.getDeviceType());
            if (current == null || (expectedConnection != null && current != expectedConnection)) {
                return null;
            }
            removed = connections.remove(user.getDeviceType());
            // Keep the user's empty-directory cleanup atomic with registration.
            if (connections.isEmpty()) {
                registry.remove(key);
            }
        }
        removed.closeConnection();
        return removed;
    }
}
