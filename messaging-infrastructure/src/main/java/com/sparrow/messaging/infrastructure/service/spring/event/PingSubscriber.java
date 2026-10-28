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

package com.sparrow.messaging.infrastructure.service.spring.event;

import com.sparrow.messaging.domain.service.ClientConnection;
import com.sparrow.messaging.domain.service.ConnectionRegistry;
import com.sparrow.messaging.infrastructure.service.spring.AbstractSpringTypedSubscriber;
import com.sparrow.messaging.protocol.Protocol;
import com.sparrow.messaging.protocol.ProtocolType;
import com.sparrow.messaging.protocol.enums.DefaultProtocolType;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PingSubscriber extends AbstractSpringTypedSubscriber<Protocol> {

    @Inject
    private ConnectionRegistry connectionRegistry;

    @Override
    public void subscribe(Protocol protocol) {
        if (protocol == null || protocol.getSender() == null || protocol.getDevice() == null) {
            throw new IllegalArgumentException("Reply requires a protocol with sender and device");
        }
        log.info("receive ping {}",protocol.getProtocolType());
        ClientConnection connection = connectionRegistry.findConnection(protocol.getSender(), protocol.getDevice());
        if (connection == null) {
            log.debug("Skip PONG: sender connection is no longer registered");
            return;
        }
        Protocol pong = Protocol.pong();
        String userKey = protocol.getSender().key();
        var device = protocol.getDevice();
        connection.sendMessage(pong.encode()).whenComplete((ignored, failure) -> {
            if (failure != null) {
                log.warn("Failed to send PONG to {} on {}", userKey, device, failure);
            }
        });
    }

    @Override
    public ProtocolType getProtocolType() {
        return DefaultProtocolType.PING;
    }
}
