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

package com.sparrow.messaging.messaging.example.file;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.sparrow.messaging.domain.service.pub.sub.mapping.PubSubMappingContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

@Component
public class FileMessageReader {
    private final Path path;
    private final PubSubMappingContainer pubSubMappingContainer;

    public FileMessageReader(@Value("${example.file.path:target/file-example/messages.jsonl}") String path,
                             PubSubMappingContainer pubSubMappingContainer) {
        this.path = Path.of(path).toAbsolutePath().normalize();
        this.pubSubMappingContainer = pubSubMappingContainer;
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
                // 订阅表按消息类名注册；file 路由只用于发布时选择文件发布器。
                var subscriber = pubSubMappingContainer.get(message.getClass().getName());
                if (subscriber == null) {
                    throw new IllegalStateException("No subscriber for event: " + message.getClass().getName());
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
            return new FileMessage(text);
        } catch (JSONException exception) {
            throw new IOException("Invalid message at " + path + ":" + lineNumber, exception);
        }
    }
}
