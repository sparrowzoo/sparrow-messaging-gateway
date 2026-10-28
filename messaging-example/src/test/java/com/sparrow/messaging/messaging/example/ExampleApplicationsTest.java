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

package com.sparrow.messaging.messaging.example;

import com.sparrow.messaging.domain.service.pub.sub.PublishStrategy;
import com.sparrow.messaging.domain.service.pub.sub.typed.TypedPublishStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ExampleApplicationsTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void ordinaryGreetingStartsAndDispatches() throws Throwable {
        try (var context = start(com.sparrow.messaging.messaging.example.greeting.MessagingExampleApplication.class)) {
            var message = new com.sparrow.messaging.messaging.example.greeting.GreetingMessage("hello");
            context.getBean(PublishStrategy.class).publish(message);
            assertSame(message, context.getBean(com.sparrow.messaging.messaging.example.greeting.GreetingSubscriber.class)
                    .getLastMessage());
        }
    }

    @Test
    void ordinaryCustomRouteReachesTheConfiguredPublisher() throws Throwable {
        try (var context = start(com.sparrow.messaging.messaging.example.routing.RoutingExampleApplication.class)) {
            var message = new com.sparrow.messaging.messaging.example.routing.RoutedMessage("hello");
            context.getBean(PublishStrategy.class).publish(message);
            assertSame(message, context.getBean(com.sparrow.messaging.messaging.example.routing.CustomPublisher.class)
                    .getLastMessage());
        }
    }

    @Test
    void ordinaryFileIsConsumedOnlyWhenExplicitlyRead() throws Throwable {
        Path file = temporaryDirectory.resolve("ordinary.jsonl");
        try (var context = start(com.sparrow.messaging.messaging.example.file.FileExampleApplication.class,
                "--example.file.path=" + file)) {
            context.getBean(PublishStrategy.class).publish(
                    new com.sparrow.messaging.messaging.example.file.FileMessage("hello"));
            var subscriber = context.getBean(com.sparrow.messaging.messaging.example.file.FileMessageSubscriber.class);
            assertTrue(Files.exists(file));
            assertNull(subscriber.getLastMessage());
            assertEquals(1, context.getBean(com.sparrow.messaging.messaging.example.file.FileMessageReader.class).readOnce());
            assertEquals("hello", subscriber.getLastMessage().text());
        }
    }

    @Test
    void typedGreetingStartsAndDispatches() throws Throwable {
        try (var context = start(com.sparrow.messaging.messaging.example.typed.greeting.MessagingExampleApplication.class)) {
            var message = new com.sparrow.messaging.messaging.example.typed.greeting.GreetingMessage("hello");
            context.getBean(TypedPublishStrategy.class).publish(message);
            assertSame(message, context.getBean(com.sparrow.messaging.messaging.example.typed.greeting.GreetingSubscriber.class)
                    .getLastMessage());
        }
    }

    @Test
    void typedCustomRouteReachesTheConfiguredPublisher() throws Throwable {
        try (var context = start(com.sparrow.messaging.messaging.example.typed.routing.RoutingExampleApplication.class)) {
            var message = new com.sparrow.messaging.messaging.example.typed.routing.RoutedMessage("hello");
            context.getBean(TypedPublishStrategy.class).publish(message);
            assertSame(message, context.getBean(com.sparrow.messaging.messaging.example.typed.routing.CustomPublisher.class)
                    .getLastMessage());
        }
    }

    @Test
    void typedFileIsConsumedOnlyWhenExplicitlyRead() throws Throwable {
        Path file = temporaryDirectory.resolve("typed.jsonl");
        try (var context = start(com.sparrow.messaging.messaging.example.typed.file.FileExampleApplication.class,
                "--example.typed.file.path=" + file)) {
            context.getBean(TypedPublishStrategy.class).publish(
                    new com.sparrow.messaging.messaging.example.typed.file.FileMessage("hello"));
            var subscriber = context.getBean(com.sparrow.messaging.messaging.example.typed.file.FileMessageSubscriber.class);
            assertTrue(Files.exists(file));
            assertNull(subscriber.getLastMessage());
            assertEquals(1, context.getBean(com.sparrow.messaging.messaging.example.typed.file.FileMessageReader.class).readOnce());
            assertEquals("hello", subscriber.getLastMessage().text());
        }
    }

    private ConfigurableApplicationContext start(Class<?> application, String... extraArguments) {
        var arguments = new java.util.ArrayList<String>();
        arguments.add("--spring.main.web-application-type=none");
        arguments.add("--spring.main.banner-mode=off");
        arguments.add("--logging.level.root=ERROR");
        arguments.addAll(java.util.List.of(extraArguments));
        return SpringApplication.run(application, arguments.toArray(String[]::new));
    }
}
