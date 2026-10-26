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

import com.alibaba.fastjson.JSON;
import com.sparrow.messaging.protocol.TypedEvent;
import com.sparrow.messaging.protocol.api.TypedPublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

@Component("fileTypedPublisher")
public class FilePublisher implements TypedPublisher {
    private final Path path;

    public FilePublisher(@Value("${example.typed.file.path:target/typed-file-example/messages.jsonl}") String path) {
        this.path = Path.of(path).toAbsolutePath().normalize();
    }

    @Override
    public void publish(TypedEvent event) throws IOException {
        if (!(event instanceof FileMessage message) || message.text() == null) {
            throw new IllegalArgumentException("FilePublisher requires a FileMessage with non-null text");
        }
        Files.createDirectories(path.getParent());
        String line = JSON.toJSONString(Map.of("text", message.text(),
                "messageType", message.getMessageType(), "eventType", message.getEventType().getIdentity())) + "\n";
        Files.writeString(path, line, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
