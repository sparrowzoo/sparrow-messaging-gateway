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

import com.sparrow.messaging.protocol.Event;
import com.sparrow.messaging.protocol.api.EventHandlerMappingContainer;
import com.sparrow.messaging.protocol.api.SubHandler;
import com.sparrow.pub.sub.MQClient;
import com.sparrow.pub.sub.MQIdempotent;
import com.sparrow.rocketmq.MessageConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

@Slf4j
public class SparrowRocketMQMessageListener implements MessageListenerConcurrently {

    public SparrowRocketMQMessageListener() {
        log.info("init spring rocket mq message listener");
    }

    private EventHandlerMappingContainer queueHandlerMappingContainer;
    private MessageConverter messageConverter;
    private MQIdempotent mqIdempotent;

    public void setQueueHandlerMappingContainer(EventHandlerMappingContainer queueHandlerMappingContainer) {
        this.queueHandlerMappingContainer = queueHandlerMappingContainer;
    }

    public void setMqIdempotent(MQIdempotent mqIdempotent) {
        this.mqIdempotent = mqIdempotent;
    }

    public void setMessageConverter(MessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }

    protected boolean duplicate(Event event, String keys) {
        return mqIdempotent != null && mqIdempotent.duplicate(keys);
    }


    protected long getLockMills() {
        return 1000 * 60;
    }

    protected int getIdempotentSeconds() {
        return 60 * 60 * 72;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
        log.info("thread-name:{},message-size:{}", Thread.currentThread().getName(), list.size());
        for (MessageExt message : list) {
            String type = message.getProperties().get(MQClient.CLASS_NAME);
            try {
                if (log.isInfoEnabled()) {
                    log.info("receive msg:" + message);
                }
                SubHandler handler = queueHandlerMappingContainer.get(type);
                if (handler == null) {
                    log.error("handler of this type [{}] not found", type);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                Event event = messageConverter.fromMessage(message);
                //确定已经成功消费则返回
                if (this.mqIdempotent.duplicate(message.getKeys())) {
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }

                //尝试拿锁失败
                if (!this.mqIdempotent.tryLock(message.getKeys(), this.getLockMills())) {
                    //已经确定消费成功
                    if (this.mqIdempotent.duplicate(message.getKeys())) {
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                    //未消费成功
                    else {
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                handler.handle(event);
                this.mqIdempotent.consumed(message.getKeys(), this.getIdempotentSeconds());
            } catch (Throwable e) {
                log.error("process failed, msg : " + message, e);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
