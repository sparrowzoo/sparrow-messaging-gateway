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

package com.sparrow.messaging.domain.service;

import com.sparrow.messaging.protocol.TypedEvent;
import com.sparrow.messaging.protocol.api.TypedPublisher;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import lombok.extern.slf4j.Slf4j;

@Named("default")
@Slf4j
public class RoutablePublisher implements TypedPublisher {

    @Inject
    private RoutableTypedHandlerMappingContainer routableTypedHandlerMappingContainer;

    @Override
    public void publish(TypedEvent event) throws Throwable {
        var handler = this.routableTypedHandlerMappingContainer.getMessageHandler(event);
        if (handler == null) {
            throw new IllegalStateException("No handler for event type: " + event.getEventType());
        }
        handler.handle(event);
    }
}
