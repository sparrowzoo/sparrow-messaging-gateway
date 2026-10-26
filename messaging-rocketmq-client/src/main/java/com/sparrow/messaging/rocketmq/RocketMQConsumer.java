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

package com.sparrow.messaging.rocketmq;

import com.sparrow.container.Container;
import com.sparrow.container.ContainerAware;
import com.sparrow.lang.joiner.StringJoiner;
import com.sparrow.protocol.Pair;
import com.sparrow.protocol.constant.magic.Symbol;
import com.sparrow.pub.sub.MQClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UnknownFormatConversionException;

@Slf4j
public class RocketMQConsumer implements ContainerAware {

    private String instanceName;

    private String groupName;

    private MessageListenerConcurrently messageListener;

    private List<TopicTagPair> topicConfigList;

    private String topicConfig;

    private String messageModel = "CLUSTERING";

    private String consumeFromLastOffset = "CONSUME_FROM_LAST_OFFSET";

    private String nameServerAddress;

    private Integer threadCount;

    private int batchSize = 1;
    private int pullSize;

    private long pullInterval = 0;

    /**
     * 消费线程的数量
     */
    protected static final Integer DEFAULT_CONSUMER_THREAD_NUM = 20;

    public Integer getThreadCount() {
        if (threadCount == null) {
            this.threadCount = DEFAULT_CONSUMER_THREAD_NUM;
        }
        return threadCount;
    }

    public void setThreadCount(Integer threadCount) {
        this.threadCount = threadCount;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public void setMessageListener(MessageListenerConcurrently messageListener) {
        this.messageListener = messageListener;
    }

    /**
     * 格式 topic1:tag1,tag1,tag3|topic2:tag1,tag2,tag3
     *
     * @param topicConfig
     */
    public void setTopicConfig(String topicConfig) {
        this.topicConfig = topicConfig;
    }

    public void setMessageModel(String messageModel) {
        this.messageModel = messageModel;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setPullSize(int pullSize) {
        this.pullSize = pullSize;
    }

    public void setPullInterval(long pullInterval) {
        this.pullInterval = pullInterval;
    }

    public List<TopicTagPair> getTopicConfigList() {
        if (this.topicConfigList != null) {
            return this.topicConfigList;
        }
        this.topicConfigList = new ArrayList<TopicTagPair>();
        if (!topicConfig.contains(Symbol.COLON)) {
            throw new UnknownFormatConversionException("format error for example topic1:tag1,tag1,tag3|topic2:tag1,tag2,tag3");
        }
        String[] topicArray = topicConfig.split("\\|");
        for (String topic : topicArray) {
            TopicTagPair topicTagPair = new TopicTagPair();
            Pair<String, String> topicTagsPair = Pair.split(topic, Symbol.COLON);
            topicTagPair.setTopic(topicTagsPair.getFirst());
            String[] tagArray = topicTagsPair.getSecond().split(",");
            topicTagPair.setTags(Arrays.asList(tagArray));
            this.topicConfigList.add(topicTagPair);
        }
        return topicConfigList;
    }

    public void setConsumeFromLastOffset(String consumeFromLastOffset) {
        this.consumeFromLastOffset = consumeFromLastOffset;

    }

    public String getNameServerAddress() {
        return nameServerAddress;
    }

    public void setNameServerAddress(String nameServerAddress) {
        this.nameServerAddress = nameServerAddress;
    }

    public String getInstanceName() {
        if (this.instanceName == null) {
            this.instanceName = MQClient.INSTANCE_NAME;
        }
        return instanceName;
    }

    private volatile MQPushConsumer consumer;

    public synchronized void start() {
        if (this.consumer != null) {
            return;
        }
        this.consumer = startConsumer(this.batchSize, this.pullSize);
    }

    private MQPushConsumer startConsumer(int batchSize, int pullSize) {
        MQPushConsumer candidate = null;
        try {
            candidate = createConsumer(batchSize, pullSize);
            candidate.start();
            return candidate;
        } catch (Exception failure) {
            cleanupFailedConsumer(candidate, failure);
            throw new IllegalStateException("Failed to start RocketMQ consumer", failure);
        }
    }

    private void cleanupFailedConsumer(MQPushConsumer candidate, Throwable failure) {
        if (candidate != null) {
            try {
                candidate.shutdown();
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
    }

    protected MQPushConsumer createConsumer() {
        return createConsumer(null, null);
    }

    private void setConsumeThread(DefaultMQPushConsumer consumer) {
        int threads = this.getThreadCount();
        if (threads <= 0) {
            throw new IllegalArgumentException("Consumer thread count must be positive");
        }
        consumer.setConsumeThreadMax(threads);
        consumer.setConsumeThreadMin(threads);
    }

    protected MQPushConsumer createConsumer(Integer batchSize, Integer pullSize) {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer(groupName);
        try {
            MessageModel messageModel = MessageModel.CLUSTERING;
            if (this.messageModel != null) {
                messageModel = MessageModel.valueOf(this.messageModel);
            }
            defaultMQPushConsumer.setMessageModel(messageModel);
            ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;
            if (this.consumeFromLastOffset != null) {
                consumeFromWhere = ConsumeFromWhere.valueOf(this.consumeFromLastOffset);
            }
            defaultMQPushConsumer.setConsumeFromWhere(consumeFromWhere);
            defaultMQPushConsumer.setNamesrvAddr(nameServerAddress);
            if (batchSize != null && batchSize > 0) {
                defaultMQPushConsumer.setConsumeMessageBatchMaxSize(batchSize);
            }
            if (pullSize != null && pullSize > 0) {
                defaultMQPushConsumer.setPullBatchSize(pullSize);
            }
            //订阅多个topic
            for (TopicTagPair topicTagPair : this.getTopicConfigList()) {
                defaultMQPushConsumer.subscribe(topicTagPair.getTopic(), new StringJoiner(Symbol.VERTICAL_LINE, topicTagPair.getTags()).join());
            }
            defaultMQPushConsumer.setInstanceName(getInstanceName() + Symbol.UNDERLINE + groupName);
            defaultMQPushConsumer.registerMessageListener(messageListener);
            defaultMQPushConsumer.setPullInterval(this.pullInterval);
            setConsumeThread(defaultMQPushConsumer);
            log.info("finished rocket mq client start!");
            return defaultMQPushConsumer;
        } catch (Exception e) {
            cleanupFailedConsumer(defaultMQPushConsumer, e);
            throw new IllegalStateException("Failed to configure RocketMQ consumer", e);
        }
    }

    public synchronized void setPullSize(int batchSize, int pullSize) {
        if (this.consumer == null) {
            this.batchSize = batchSize;
            this.pullSize = pullSize;
            return;
        }
        int previousBatchSize = this.batchSize;
        int previousPullSize = this.pullSize;
        // 关闭失败时仍保留原引用，避免在状态未知时创建第二个实例。
        this.consumer.shutdown();
        this.consumer = null;
        try {
            this.consumer = startConsumer(batchSize, pullSize);
            this.batchSize = batchSize;
            this.pullSize = pullSize;
        } catch (RuntimeException adjustmentFailure) {
            try {
                this.consumer = startConsumer(previousBatchSize, previousPullSize);
            } catch (RuntimeException recoveryFailure) {
                adjustmentFailure.addSuppressed(recoveryFailure);
            }
            throw adjustmentFailure;
        }
    }

    public synchronized void shutdown() {
        if (this.consumer != null) {
            this.consumer.shutdown();
            this.consumer = null;
        }
    }

    @Override
    public void aware(Container container, String beanName) {
        this.start();
    }

    public void setTopicConfigList(List<TopicTagPair> topicConfigList) {
        this.topicConfigList = topicConfigList;
    }
}
