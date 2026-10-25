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

package com.sparrow.messaging.adapter.controller;

import com.sparrow.authenticator.Authenticator;
import com.sparrow.authenticator.DefaultLoginUser;
import com.sparrow.authenticator.HostAuthenticationToken;
import com.sparrow.authenticator.token.BearerToken;
import com.sparrow.messaging.domain.service.ClientConnection;
import com.sparrow.messaging.domain.service.ConnectionRegistry;
import com.sparrow.messaging.protocol.query.ChatUserQuery;
import com.sparrow.context.SessionContext;
import com.sparrow.protocol.BusinessException;
import com.sparrow.protocol.ClientInformation;
import com.sparrow.protocol.LoginUser;
import com.sparrow.protocol.enums.DeviceType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/chat/v2")
public class ConnectionController {
    @Autowired
    private Authenticator authenticator;

    @Autowired
    private ConnectionRegistry connectionRegistry;

    @PostMapping("/parse-token.json")
    public LoginUser parseToken(String token) throws BusinessException {
        ClientInformation client = SessionContext.getClientInfo();
        HostAuthenticationToken hostAuthenticationToken = new BearerToken(token, client.getDeviceId());
        return this.authenticator.authenticate(hostAuthenticationToken);
    }

    @GetMapping("/get-current-user.json")
    public LoginUser getCurrentUser() {
        return SessionContext.getLoginUser();
    }


    @PostMapping("/is-online.json")
    public Boolean online(ChatUserQuery chatUser) throws BusinessException {
        DefaultLoginUser loginUser = new DefaultLoginUser();
        loginUser.setUserId(chatUser.toChatUser().getLongUserId());
        loginUser.setCategory(chatUser.getCategory());
        Map<DeviceType, ClientConnection> connections = connectionRegistry.findConnections(loginUser);
        return connections != null && !connections.isEmpty();
    }


}
