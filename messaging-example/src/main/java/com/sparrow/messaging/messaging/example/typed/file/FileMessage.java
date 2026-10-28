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

package com.sparrow.messaging.messaging.example.typed.file;

import com.sparrow.messaging.messaging.example.typed.ExampleMessageType;
import com.sparrow.messaging.protocol.ProtocolType;
import com.sparrow.messaging.protocol.TypedEvent;
import com.sparrow.messaging.protocol.enums.DefaultProtocolType;

public record FileMessage(String text, short messageType) implements TypedEvent {
    public FileMessage(String text) {
        this(text, ExampleMessageType.FILE.getIdentity().shortValue());
    }

    @Override
    public String getRoutingKey() {
        return "typed-file";
    }

    @Override
    public short getMessageType() {
        return messageType;
    }

    @Override
    public ProtocolType getProtocolType() {
        return DefaultProtocolType.IM;
    }
}
