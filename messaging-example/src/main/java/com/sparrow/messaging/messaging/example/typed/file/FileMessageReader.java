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
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.sparrow.messaging.domain.service.pub.sub.typed.mapping.TypedPubSubMappingContainer;
import com.sparrow.messaging.protocol.enums.DefaultEventType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

@Component
public class FileMessageReader {
    private final Path path;
    private final TypedPubSubMappingContainer typedPubSubMappingContainer;

    public FileMessageReader(@Value("${example.typed.file.path:target/typed-file-example/messages.jsonl}") String path,
                             TypedPubSubMappingContainer typedPubSubMappingContainer) {
        this.path = Path.of(path).toAbsolutePath().normalize();
        this.typedPubSubMappingContainer = typedPubSubMappingContainer;
    }

    /**
     * 从头读取一次，保留原文件；再次调用会重新订阅全部历史消息。
     */
    public int readOnce() throws Throwable {
        if (Files.notExists(path)) {
            return 0;
        }
        int count = 0;
        try (var reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                FileMessage message = decode(line, count + 1);
                // 按文件中保存的消息类型直接找订阅者；读取过程不依赖 Publisher。
                var subscriber = typedPubSubMappingContainer.getMessageHandler(message);
                if (subscriber == null) {
                    throw new IllegalStateException("No subscriber for message type: " + message.getMessageType());
                }
                subscriber.subscribe(message);
                count++;
            }
        }
        return count;
    }

    private FileMessage decode(String line, int lineNumber) throws IOException {
        try {
            JSONObject json = JSON.parseObject(line);
            if (json == null || !(json.get("text") instanceof String text)) {
                throw new IOException("Missing string text at " + path + ":" + lineNumber);
            }
            if (!(json.get("messageType") instanceof Integer messageType)
                    || messageType < Short.MIN_VALUE || messageType > Short.MAX_VALUE
                    || !DefaultEventType.MESSAGE.getIdentity().equals(json.get("eventType"))) {
                throw new IOException("Invalid message type at " + path + ":" + lineNumber);
            }
            return new FileMessage(text, messageType.shortValue());
        } catch (JSONException exception) {
            throw new IOException("Invalid message at " + path + ":" + lineNumber, exception);
        }
    }
}
