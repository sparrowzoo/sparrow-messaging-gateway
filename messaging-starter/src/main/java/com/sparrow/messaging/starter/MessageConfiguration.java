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

import com.sparrow.messaging.domain.service.ConnectionRegistry;
import com.sparrow.messaging.domain.service.MultiDeviceConnectionRegistry;
import com.sparrow.messaging.domain.service.pub.sub.DefaultPublisher;
import com.sparrow.messaging.domain.service.pub.sub.PublishStrategy;
import com.sparrow.messaging.domain.service.pub.sub.RoutingKeyPublisherMapping;
import com.sparrow.messaging.domain.service.pub.sub.mapping.PubSubMappingContainer;
import com.sparrow.messaging.domain.service.pub.sub.typed.DefaultTypedPublisher;
import com.sparrow.messaging.domain.service.pub.sub.typed.TypedPublishStrategy;
import com.sparrow.messaging.domain.service.pub.sub.typed.mapping.TypedPubSubMappingContainer;
import com.sparrow.messaging.infrastructure.service.spring.LocalConfigRoutingKeyPublisherMapping;
import com.sparrow.messaging.infrastructure.service.spring.event.AIEventSubscriber;
import com.sparrow.messaging.infrastructure.service.spring.event.IMEventSubscriber;
import com.sparrow.messaging.infrastructure.service.spring.event.PingSubscriber;
import com.sparrow.messaging.infrastructure.service.spring.event.UserStatusEventSubscriber;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.SearchStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class MessageConfiguration {
    @Bean
    public DefaultPublisher defaultPublisher() {
        return new DefaultPublisher();
    }

    @Bean
    public DefaultTypedPublisher defaultTypedPublisher() {
        return new DefaultTypedPublisher();
    }

    @Bean
    public PingSubscriber pingSubscriber() {
        return new PingSubscriber();
    }

    @Bean
    @ConditionalOnMissingBean(name = "imEventSubscriber")
    public IMEventSubscriber imEventSubscriber() {
        return new IMEventSubscriber();
    }

    @Bean
    @ConditionalOnMissingBean(name = "aiEventSubscriber")
    public AIEventSubscriber aiEventSubscriber() {
        return new AIEventSubscriber();
    }

    @Bean
    @ConditionalOnMissingBean(name = "userStatusEventSubscriber")
    public UserStatusEventSubscriber userStatusEventSubscriber() {
        return new UserStatusEventSubscriber();
    }

    @Bean
    public RoutingKeyPublisherMapping routingKeyPublisherMapping() {
        return new LocalConfigRoutingKeyPublisherMapping();
    }

    @Bean
    @ConditionalOnMissingBean(ConnectionRegistry.class)
    public ConnectionRegistry connectionRegistry() {
        return new MultiDeviceConnectionRegistry();
    }

    @Bean
    @ConditionalOnMissingBean(name = "pubSubMappingContainer", search = SearchStrategy.CURRENT)
    public PubSubMappingContainer pubSubMappingContainer() {
        return new PubSubMappingContainer();
    }

    @Bean
    @ConditionalOnMissingBean(name = "typedPubSubMappingContainer", search = SearchStrategy.CURRENT)
    public TypedPubSubMappingContainer typedPubSubMappingContainer() {
        return new TypedPubSubMappingContainer();
    }

    @Bean
    public PublishStrategy publishStrategy() {
        return new PublishStrategy();
    }

    @Bean
    public TypedPublishStrategy typedPublishStrategy() {
        return new TypedPublishStrategy();
    }
}
