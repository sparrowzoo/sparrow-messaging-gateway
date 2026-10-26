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
import com.sparrow.authenticator.DefaultLoginUser;
import com.sparrow.protocol.LoginUser;
import com.sparrow.protocol.enums.DeviceType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MultiDeviceConnectionRegistryTest {
    @Test
    void missingUsersReturnEmptySnapshots() throws Exception {
        var registry = new MultiDeviceConnectionRegistry();
        assertEquals(Map.of(), registry.findConnections(user(1)));
        assertEquals(Map.of(), registry.findConnections(1L));
        assertEquals(Map.of(), registry.findConnections(ChatUser.longUserId(1L, 1)));
    }

    @Test
    void snapshotsCannotMutateTheDirectoryAndDoNotChangeLater() throws Exception {
        var registry = new MultiDeviceConnectionRegistry();
        var first = new Connection(1);
        registry.register(first);
        var snapshot = registry.findConnections(first.user);
        var all = registry.getAll();
        assertThrows(UnsupportedOperationException.class, snapshot::clear);
        assertThrows(UnsupportedOperationException.class, all::clear);
        assertThrows(UnsupportedOperationException.class, () -> all.values().iterator().next().clear());
        var second = new Connection(1);
        registry.register(second);
        assertSame(first, snapshot.values().iterator().next());
        assertSame(second, registry.findConnection(first.user));
    }

    @Test
    void repeatedRemovalIsSafeAndAllDisconnectedUsersAreRemoved() {
        var registry = new MultiDeviceConnectionRegistry();
        for (int i = 0; i < 10000; i++) {
            var connection = new Connection(i);
            registry.register(connection);
            assertSame(connection, registry.removeConnection(connection.user));
            assertNull(registry.removeConnection(connection.user));
            assertEquals(1, connection.closed.get());
        }
        assertTrue(registry.getAll().isEmpty());
    }

    @Test
    void registeringSameInstanceDoesNotReturnItForClosing() {
        var registry = new MultiDeviceConnectionRegistry();
        var connection = new Connection(1);
        registry.register(connection);
        assertNull(registry.register(connection));
    }

    @Test
    void staleCloseCannotRemoveReplacement() throws Exception {
        var registry = new MultiDeviceConnectionRegistry();
        var old = new Connection(1);
        var replacement = new Connection(1);
        registry.register(old);
        registry.register(replacement);
        assertNull(registry.removeConnection(old.user, old));
        assertSame(replacement, registry.findConnection(old.user));
        assertEquals(0, replacement.closed.get());
        assertSame(replacement, registry.removeConnection(replacement.user, replacement));
        assertTrue(registry.getAll().isEmpty());
    }

    @Test
    void concurrentRegistrationsFormOneReplacementChain() throws Exception {
        var registry = new MultiDeviceConnectionRegistry();
        try (var pool = Executors.newFixedThreadPool(8)) {
            for (int round = 0; round < 50; round++) {
                var barrier = new CyclicBarrier(8);
                List<Connection> connections = new ArrayList<>();
                List<Future<ClientConnection>> results = new ArrayList<>();
                for (int i = 0; i < 8; i++) {
                    var connection = new Connection(round);
                    connections.add(connection);
                    results.add(pool.submit(() -> {
                        barrier.await(5, TimeUnit.SECONDS);
                        return registry.register(connection);
                    }));
                }
                var replaced = new HashSet<ClientConnection>();
                for (var result : results) {
                    var old = result.get(5, TimeUnit.SECONDS);
                    if (old != null) {
                        assertTrue(replaced.add(old), "Each previous connection may be replaced only once");
                    }
                }
                assertEquals(7, replaced.size());
                var current = registry.findConnection(connections.getFirst().user);
                assertFalse(replaced.contains(current));
                replaced.add(current);
                assertEquals(new HashSet<>(connections), replaced);
            }
        }
    }

    @Test
    void closeRunsAfterRemovalAndOutsideDirectoryLock() {
        var registry = new MultiDeviceConnectionRegistry();
        var connection = new Connection(1) {
            @Override
            public void closeConnection() {
                assertDoesNotThrow(() -> CompletableFuture.runAsync(() -> {
                    assertTrue(registry.getAll().isEmpty());
                    registry.register(new Connection(2));
                }).get(5, TimeUnit.SECONDS));
                super.closeConnection();
            }
        };
        registry.register(connection);
        registry.removeConnection(connection.user);
    }

    @Test
    void concurrentCloseAndRegistrationAlwaysKeepTheReplacement() throws Exception {
        var registry = new MultiDeviceConnectionRegistry();
        try (var pool = Executors.newFixedThreadPool(2)) {
            for (int i = 0; i < 100; i++) {
                var old = new Connection(i);
                var replacement = new Connection(i);
                registry.register(old);
                var barrier = new CyclicBarrier(2);
                var closing = pool.submit(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    return registry.removeConnection(old.user, old);
                });
                var registering = pool.submit(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    return registry.register(replacement);
                });
                closing.get(5, TimeUnit.SECONDS);
                registering.get(5, TimeUnit.SECONDS);
                assertSame(replacement, registry.findConnection(old.user));
                assertEquals(0, replacement.closed.get());
            }
        }
    }

    @Test
    void closeFailureDoesNotRestoreTheRemovedDirectoryEntry() {
        var registry = new MultiDeviceConnectionRegistry();
        var connection = new Connection(1) {
            @Override
            public void closeConnection() {
                throw new IllegalStateException("close failed");
            }
        };
        registry.register(connection);
        assertThrows(IllegalStateException.class, () -> registry.removeConnection(connection.user, connection));
        assertTrue(registry.getAll().isEmpty());
    }

    @Test
    void recentConnectionToleratesMissingTimestamp() {
        var registry = new MultiDeviceConnectionRegistry();
        var first = new Connection(1);
        var second = new Connection(1);
        ((DefaultLoginUser) second.user).setDeviceType(DeviceType.values()[1]);
        second.access = null;
        registry.register(first);
        registry.register(second);
        assertSame(first, registry.getRecentlyConnection(ChatUser.longUserId(1L, 1)));
    }

    static LoginUser user(long id) {
        var user = new DefaultLoginUser();
        user.setUserId(id);
        user.setCategory(1);
        user.setDeviceType(DeviceType.values()[0]);
        return user;
    }

    static class Connection implements ClientConnection {
        final LoginUser user;
        final AtomicInteger closed = new AtomicInteger();
        Long access = 10L;
        Connection(long id) { user = user(id); }
        public LoginUser loginUser() { return user; }
        public CompletionStage<Void> sendMessage(ByteBuffer frame) { return CompletableFuture.completedFuture(null); }
        public void closeConnection() { closed.incrementAndGet(); }
        public Long getLastAccessTime() { return access; }
        public void refreshAccessTime() { access = 10L; }
        public Long getCreateTime() { return 0L; }
        public Long getDuration() { return 10L; }
    }
}
