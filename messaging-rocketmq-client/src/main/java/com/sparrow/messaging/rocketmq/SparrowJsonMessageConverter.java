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

package com.sparrow.rocketmq.impl;

import com.sparrow.core.spi.JsonFactory;
import com.sparrow.messaging.protocol.Event;
import com.sparrow.pub.sub.MQClient;
import com.sparrow.rocketmq.MessageConverter;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.Message;

import java.io.UnsupportedEncodingException;

@Data
@Slf4j
public class SparrowJsonMessageConverter implements MessageConverter {

    private String charset;

    @Override
    public Event fromMessage(Message message) {
        String json = null;
        try {
            json = new String(message.getBody(), charset);
        } catch (UnsupportedEncodingException e) {
            log.error("unsupported encodingException {}", charset);
            throw new RuntimeException(e);
        }

        String className = message.getProperties().get(MQClient.CLASS_NAME);
        try {
            return (Event) JsonFactory.getProvider().parse(json, Class.forName(className));
        } catch (ClassNotFoundException e) {
            log.error("class{} not found", className);
            return null;
        }
    }

    @Override
    public Message createMessage(String topic, String tag, Event event) {
        String jsonString = JsonFactory.getProvider().toString(event);
        try {
            byte[] bytes = jsonString.getBytes(charset);
            Message message = new Message(topic, tag, bytes);
            message.getProperties().put(MQClient.CLASS_NAME, event.getClass().getName());
            return message;
        } catch (UnsupportedEncodingException e) {
            log.error("unsupported encodingException {}", charset);
            throw new RuntimeException(e);
        }
    }
}
