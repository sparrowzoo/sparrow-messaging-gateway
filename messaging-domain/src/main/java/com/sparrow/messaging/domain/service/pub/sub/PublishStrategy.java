/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sparrow.messaging.domain.service.pub.sub;

import com.sparrow.messaging.protocol.Event;
import com.sparrow.messaging.protocol.api.Publisher;
import jakarta.inject.Inject;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor
public class PublishStrategy implements Publisher {
    @Inject
    private Map<String, Publisher> publisherMap = new HashMap<>();

    @Inject
    private RoutingKeyPublisherMapping routingKeyPublisherMapping;

    @Override
    public void publish(Event event) throws Throwable {
        String routingKey = event.getRoutingKey();
        String publisherKey = this.routingKeyPublisherMapping.getPublisherKey(routingKey);
        Publisher publisher = publisherMap.get(publisherKey);
        if (publisher == null) {
            publisher = publisherMap.get("defaultPublisher");
        }
        if (publisher == null) {
            throw new IllegalAccessException("default publish not found");
        }
        publisher.publish(event);
    }
}
