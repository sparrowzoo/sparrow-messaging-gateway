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

package com.sparrow.messaging.messaging.example.routing;

import com.sparrow.messaging.domain.service.pub.sub.DefaultPublisher;
import com.sparrow.messaging.protocol.Event;
import com.sparrow.messaging.protocol.api.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

@Component("customPublisher")
public class CustomPublisher implements Publisher {
    private static final Logger logger = LoggerFactory.getLogger(CustomPublisher.class);
    private final DefaultPublisher defaultPublisher;
    private final AtomicReference<Event> lastMessage = new AtomicReference<>();

    public CustomPublisher(DefaultPublisher defaultPublisher) {
        this.defaultPublisher = defaultPublisher;
    }

    @Override
    public void publish(Event event) throws Throwable {
        logger.info("自定义发布器收到路由消息：{}", event.getRoutingKey());
        defaultPublisher.publish(event);
        lastMessage.set(event);
    }

    public Event getLastMessage() {
        return lastMessage.get();
    }
}
