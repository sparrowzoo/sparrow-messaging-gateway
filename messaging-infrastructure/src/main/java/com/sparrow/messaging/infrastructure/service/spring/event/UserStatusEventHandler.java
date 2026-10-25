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
import com.sparrow.messaging.infrastructure.service.spring.AbstractSpringTypedHandler;
import com.sparrow.messaging.protocol.EventType;
import com.sparrow.messaging.protocol.Protocol;
import com.sparrow.messaging.protocol.enums.DefaultEventType;
import jakarta.inject.Inject;

public class UserStatusEventHandler extends AbstractSpringTypedHandler<Protocol> {
    @Inject
    private ConnectionRegistry connectionRegistry;

    @Override
    public void handle(Protocol protocol) throws Throwable {
        Protocol userStatus = protocol.clone();
        userStatus.setPayload("联系人在线状态");
        ClientConnection clientConnection = this.connectionRegistry.findConnection(protocol.getSender(), protocol.getDevice());
        clientConnection.sendMessage(userStatus.encode());
    }

    @Override
    public EventType getEventType() {
        return DefaultEventType.USER_STATUS;
    }
}
