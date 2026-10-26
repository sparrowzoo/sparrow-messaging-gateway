package com.sparrow.rocketmq.impl;

import com.sparrow.messaging.protocol.Event;
import com.sparrow.messaging.protocol.api.EventHandlerMappingContainer;
import com.sparrow.messaging.protocol.api.Subscriber;
import com.sparrow.messaging.rocketmq.MessageConverter;
import com.sparrow.pub.sub.MQClient;
import com.sparrow.pub.sub.MQIdempotent;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus.*;

class SparrowRocketMQMessageListenerTest {
    private final List<Event> delivered = new ArrayList<>();
    private final Event event = new Event() {};

    private SparrowRocketMQMessageListener listener(MQIdempotent idempotent) {
        var listener = new SparrowRocketMQMessageListener();
        listener.setMqIdempotent(idempotent);
        listener.setQueueHandlerMappingContainer(new EventHandlerMappingContainer() {
            public void put(Subscriber handler) {}
            public void put(Class<? extends Event> eventClass, Subscriber handler) {}
            public Subscriber get(String key) { return (Subscriber<Event>) delivered::add; }
        });
        listener.setMessageConverter(new MessageConverter() {
            public Event fromMessage(Message message) { return event; }
            public Message createMessage(String topic, String tag, Event event) { return null; }
        });
        return listener;
    }

    private static MessageExt message(String key) {
        var message = new MessageExt();
        message.setKeys(key);
        message.getProperties().put(MQClient.CLASS_NAME, Event.class.getName());
        return message;
    }

    @Test
    void duplicateDoesNotAcknowledgeUnprocessedBatchTail() {
        var listener = listener(new Idempotent() {
            public boolean duplicate(String key) { return key.equals("old"); }
        });
        assertEquals(CONSUME_SUCCESS, listener.consumeMessage(List.of(message("old"), message("new")), null));
        assertEquals(List.of(event), delivered);
    }

    @Test
    void duplicateDetectedAfterLockFailureDoesNotSkipBatchTail() {
        var checks = new AtomicInteger();
        var listener = listener(new Idempotent() {
            public boolean duplicate(String key) { return key.equals("old") && checks.incrementAndGet() > 1; }
            public boolean tryLock(String key, Long millis) { return !key.equals("old"); }
        });
        assertEquals(CONSUME_SUCCESS, listener.consumeMessage(List.of(message("old"), message("new")), null));
        assertEquals(List.of(event), delivered);
    }

    @Test
    void optionalIdempotenceDoesNotPreventDelivery() {
        assertEquals(CONSUME_SUCCESS, listener(null).consumeMessage(List.of(message("new")), null));
        assertEquals(List.of(event), delivered);
    }

    @Test
    void unavailableLockWithoutDuplicateDoesNotAcknowledge() {
        var listener = listener(new Idempotent() {
            public boolean tryLock(String key, Long millis) { return false; }
        });
        assertEquals(RECONSUME_LATER, listener.consumeMessage(List.of(message("new")), null));
        assertEquals(List.of(), delivered);
    }

    @Test
    void subscriberFailureIsRetriedAndNotMarkedConsumed() {
        var consumed = new AtomicInteger();
        var listener = listener(new Idempotent() {
            public boolean consumed(String key, Integer seconds) { consumed.incrementAndGet(); return true; }
        });
        listener.setQueueHandlerMappingContainer(new EventHandlerMappingContainer() {
            public void put(Subscriber handler) {}
            public void put(Class<? extends Event> eventClass, Subscriber handler) {}
            public Subscriber get(String key) { return (Subscriber<Event>) e -> { throw new IllegalStateException("subscriber failed"); }; }
        });
        assertEquals(RECONSUME_LATER, listener.consumeMessage(List.of(message("new")), null));
        assertEquals(0, consumed.get());
    }

    private static class Idempotent implements MQIdempotent {
        public boolean duplicate(String key) { return false; }
        public boolean tryLock(String key, Long millis) { return true; }
        public boolean consumed(String key, Integer seconds) { return true; }
    }
}
