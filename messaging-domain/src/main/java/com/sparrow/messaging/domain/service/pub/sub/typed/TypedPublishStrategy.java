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

package com.sparrow.messaging.domain.service.pub.sub.typed;

import com.sparrow.messaging.domain.service.pub.sub.RoutingKeyPublisherMapping;
import com.sparrow.messaging.protocol.TypedEvent;
import com.sparrow.messaging.protocol.api.TypedPublisher;
import jakarta.inject.Inject;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@NoArgsConstructor
public class TypedPublishStrategy implements TypedPublisher {
    @Inject
    private Map<String, TypedPublisher> typedPublisherMap = new HashMap<>();

    @Inject
    private RoutingKeyPublisherMapping routingKeyPublisherMapping;

    @Override
    public void publish(TypedEvent event) throws Throwable {
        String publisherKey = this.routingKeyPublisherMapping.getPublisherKey(event);
        TypedPublisher publisher = typedPublisherMap.get(publisherKey);

        if (publisher == null) {
            publisher = typedPublisherMap.get("defaultTypedPublisher");
        }
        if (publisher == null) {
            throw new IllegalAccessException("default publish not found");
        }
        publisher.publish(event);
    }
}
