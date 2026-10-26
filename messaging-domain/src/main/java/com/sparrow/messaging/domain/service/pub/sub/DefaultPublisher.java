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

package com.sparrow.messaging.domain.service.pub.sub;

import com.sparrow.messaging.domain.service.pub.sub.mapping.PubSubMappingContainer;
import com.sparrow.messaging.protocol.Event;
import com.sparrow.messaging.protocol.api.Publisher;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import lombok.extern.slf4j.Slf4j;

@Named("defaultPublisher")
@Slf4j
public class DefaultPublisher implements Publisher {

    @Inject
    private PubSubMappingContainer pubSubMappingContainer;

    @Override
    public void publish(Event event) throws Throwable {
        var handler = this.pubSubMappingContainer.get(event.getClass().getName());
        if (handler == null) {
            throw new IllegalStateException("No handler for event: " + event.getClass().getName());
        }
        handler.subscribe(event);
    }
}
