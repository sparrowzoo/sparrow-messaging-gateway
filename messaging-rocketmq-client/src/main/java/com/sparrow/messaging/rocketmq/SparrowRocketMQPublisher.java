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

import com.sparrow.container.Container;
import com.sparrow.container.ContainerAware;
import com.sparrow.core.spi.JsonFactory;
import com.sparrow.messaging.protocol.Event;
import com.sparrow.messaging.protocol.api.Publisher;
import com.sparrow.pub.sub.MQClient;
import com.sparrow.pub.sub.MQMessageSendException;
import com.sparrow.messaging.rocketmq.MessageConverter;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.UUID;

public class SparrowRocketMQPublisher implements Publisher, ContainerAware {
    protected static Logger logger = LoggerFactory.getLogger(SparrowRocketMQPublisher.class);
    private String nameServerAddress;
    private String group;
    private String topic;
    private String tag;
    private Boolean debug;
    private MQProducer producer;
    private MessageConverter messageConverter;
    private Integer retryTimesWhenSendAsyncFailed = 5;

    public String getNameServerAddress() {
        return nameServerAddress;
    }

    public void setNameServerAddress(String nameServerAddress) {
        this.nameServerAddress = nameServerAddress;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getTopic() {
        return topic;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public MessageConverter getMessageConverter() {
        return messageConverter;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setMessageConverter(MessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }

    public Integer getRetryTimesWhenSendAsyncFailed() {
        return retryTimesWhenSendAsyncFailed;
    }

    public void setRetryTimesWhenSendAsyncFailed(Integer retryTimesWhenSendAsyncFailed) {
        this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
    }

    public void setDebug(Boolean debug) {
        this.debug = debug;
    }

    public void after(Event event, String msgKey) {
    }

    @Override
    public void publish(Event event) {
        Integer maxAttempts = this.retryTimesWhenSendAsyncFailed;
        if (maxAttempts == null || maxAttempts <= 0) {
            throw new IllegalArgumentException("Send attempt count must be positive");
        }
        Message msg = this.messageConverter.createMessage(topic, tag, event);
        String key = UUID.randomUUID().toString();
        msg.setKeys(Collections.singletonList(key));
        logger.info("event {},msgKey {}", JsonFactory.getProvider().toString(event), key);
        // 限制本类调用 send 的次数；SDK 内部可能还有自己的重试。
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                SendResult sendResult = producer.send(msg);
                if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                    throw new MQMessageSendException(sendResult.toString());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new MQMessageSendException("Send interrupted", e);
            } catch (Exception e) {
                logger.warn("Send attempt {} failed, msgKey {}", attempt, key, e);
                if (attempt == maxAttempts) {
                    throw new MQMessageSendException("client exception", e);
                }
                continue;
            }
            // 消息已发送成功；回调失败只向上报告，不能因此重发。
            this.after(event, key);
            return;
        }
    }

    public void start() throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer(group);
        producer.setNamesrvAddr(nameServerAddress);
        producer.setInstanceName(MQClient.INSTANCE_NAME);
        //for product
        if (this.debug == null || !this.debug) {
            producer.setCreateTopicKey(this.getTopic());
        }

        int maxMessageSize = 1024000;
        producer.setMaxMessageSize(maxMessageSize);
        this.producer = producer;
        producer.start();
    }

    @Override
    public void aware(Container container, String beanName) {
        try {
            this.start();
        } catch (MQClientException e) {
            logger.error("mq client exception", e);
        }
    }
}
