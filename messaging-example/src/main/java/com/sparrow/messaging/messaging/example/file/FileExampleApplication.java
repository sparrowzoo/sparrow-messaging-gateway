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

import com.sparrow.messaging.domain.service.pub.sub.PublishStrategy;
import com.sparrow.messaging.starter.EnableMessage;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@EnableMessage
@Import(PublishStrategy.class)
public class FileExampleApplication {
    public static void main(String[] args) throws Throwable {
        try (var context = SpringApplication.run(FileExampleApplication.class, args)) {
            // 第一步只写入指定文件，此时订阅者不会收到消息。
            context.getBean(PublishStrategy.class).publish(new FileMessage("Hello file messaging"));
            // 第二步从文件读取，再交给订阅者处理，完成后退出。
            context.getBean(FileMessageReader.class).readOnce();
        }
    }
}
