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
import com.sparrow.messaging.domain.service.pub.sub.RoutingKeyPublisherMapping;
import com.sparrow.messaging.infrastructure.service.spring.LocalConfigRoutingKeyPublisherMapping;
import com.sparrow.messaging.domain.service.pub.sub.mapping.PubSubMappingContainer;
import com.sparrow.messaging.domain.service.pub.sub.typed.mapping.TypedPubSubMappingContainer;
import com.sparrow.messaging.domain.service.pub.sub.DefaultPublisher;
import com.sparrow.messaging.domain.service.pub.sub.typed.DefaultTypedPublisher;
import com.sparrow.messaging.domain.service.pub.sub.typed.TypedPublishStrategy;
import com.sparrow.messaging.infrastructure.service.spring.event.MessageEventSubscriber;
import com.sparrow.messaging.infrastructure.service.spring.event.PingSubscriber;
import com.sparrow.messaging.infrastructure.service.spring.event.UserStatusEventSubscriber;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration(proxyBeanMethods = false)
@Import({DefaultPublisher.class, DefaultTypedPublisher.class, PingSubscriber.class,
            MessageEventSubscriber.class, UserStatusEventSubscriber.class})
public class MessageConfiguration {
    @Bean
    public RoutingKeyPublisherMapping routingKeyPublisherMapping() {
        return new LocalConfigRoutingKeyPublisherMapping();
    }

    @Bean
    public ConnectionRegistry connectionRegistry() {
        return new MultiDeviceConnectionRegistry();
    }

    @Bean
    public PubSubMappingContainer defaultQueueHandlerMappingContainer() {
        return new PubSubMappingContainer();
    }

    @Bean
    public TypedPubSubMappingContainer routableTypedHandlerMappingContainer() {
        return new TypedPubSubMappingContainer();
    }

    @Bean
    public TypedPublishStrategy publishStrategy() {
        return new TypedPublishStrategy();
    }
}
