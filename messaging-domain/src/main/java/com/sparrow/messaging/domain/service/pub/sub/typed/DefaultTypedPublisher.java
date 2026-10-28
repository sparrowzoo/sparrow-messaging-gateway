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

package com.sparrow.messaging.domain.service.pub.sub.typed;

import com.sparrow.messaging.domain.service.pub.sub.typed.mapping.TypedPubSubMappingContainer;
import com.sparrow.messaging.protocol.TypedEvent;
import com.sparrow.messaging.protocol.api.TypedPublisher;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultTypedPublisher implements TypedPublisher {

    @Inject
    private TypedPubSubMappingContainer typedPubSubMappingContainer;

    @Override
    public void publish(TypedEvent event) throws Throwable {
        var handler = this.typedPubSubMappingContainer.get(event);
        if (handler == null) {
            throw new IllegalStateException("No handler for message type " + Short.toUnsignedInt(event.getMessageType())
                    + ", protocol type " + event.getProtocolType());
        }
        handler.subscribe(event);
    }
}
