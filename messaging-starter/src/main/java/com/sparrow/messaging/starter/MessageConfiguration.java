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

package com.sparrow.messaging.starter;

import com.sparrow.messaging.domain.service.*;
import com.sparrow.messaging.infrastructure.service.spring.event.MessageEventHandler;
import com.sparrow.messaging.infrastructure.service.spring.event.PingHandler;
import com.sparrow.messaging.infrastructure.service.spring.event.UserStatusEventHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration(proxyBeanMethods = false)
@Import({DefaultPublisher.class, RoutablePublisher.class, PingHandler.class,
            MessageEventHandler.class, UserStatusEventHandler.class})
public class MessageConfiguration {
    @Bean
    public ConnectionRegistry connectionRegistry() {
        return new MultiDeviceConnectionRegistry();
    }

    @Bean
    public DefaultQueueHandlerMappingContainer defaultQueueHandlerMappingContainer() {
        return new DefaultQueueHandlerMappingContainer();
    }

    @Bean
    public RoutableTypedHandlerMappingContainer routableTypedHandlerMappingContainer() {
        return new RoutableTypedHandlerMappingContainer();
    }

    @Bean
    public PublishStrategy publishStrategy() {
        return new PublishStrategy();
    }
}
