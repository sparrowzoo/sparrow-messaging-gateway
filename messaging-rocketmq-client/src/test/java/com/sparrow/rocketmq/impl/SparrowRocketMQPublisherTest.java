package com.sparrow.rocketmq.impl;

import com.sparrow.messaging.protocol.Event;
import com.sparrow.messaging.rocketmq.MessageConverter;
import com.sparrow.pub.sub.MQMessageSendException;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SparrowRocketMQPublisherTest {
    private final Event event = new Event() {};
    private final List<Message> sends = new ArrayList<>();

    private SparrowRocketMQPublisher publisher(Integer attempts, Throwable failure, int failBeforeSuccess) throws Exception {
        return configure(new SparrowRocketMQPublisher(), attempts, failure, failBeforeSuccess);
    }

    private SparrowRocketMQPublisher configure(SparrowRocketMQPublisher publisher, Integer attempts,
                                               Throwable failure, int failBeforeSuccess) throws Exception {
        publisher.setRetryTimesWhenSendAsyncFailed(attempts);
        publisher.setMessageConverter(new MessageConverter() {
            public Event fromMessage(Message message) { return event; }
            public Message createMessage(String topic, String tag, Event event) { return new Message("topic", new byte[]{1}); }
        });
        var producer = (MQProducer) Proxy.newProxyInstance(MQProducer.class.getClassLoader(), new Class[]{MQProducer.class}, (p, method, args) -> {
            if (method.getName().equals("send")) {
                sends.add((Message) args[0]);
                if (sends.size() <= failBeforeSuccess) { throw failure; }
                var result = new SendResult();
                result.setSendStatus(SendStatus.SEND_OK);
                return result;
            }
            return null;
        });
        var field = SparrowRocketMQPublisher.class.getDeclaredField("producer");
        field.setAccessible(true);
        field.set(publisher, producer);
        return publisher;
    }

    @Test
    void oneAttemptFailureIsReported() throws Exception {
        var publisher = publisher(1, new IllegalStateException("send failed"), 1);
        assertThrows(MQMessageSendException.class, () -> publisher.publish(event));
        assertEquals(1, sends.size());
    }

    @Test
    void usesAllConfiguredAttemptsAndKeepsSameMessageKey() throws Exception {
        var publisher = publisher(5, new IllegalStateException("send failed"), 5);
        assertThrows(MQMessageSendException.class, () -> publisher.publish(event));
        assertEquals(5, sends.size());
        assertNotNull(sends.getFirst().getKeys());
        assertTrue(sends.stream().allMatch(m -> m == sends.getFirst()));
    }

    @Test
    void invalidAttemptCountsDoNotSend() throws Exception {
        for (Integer attempts : new Integer[]{0, -1, null}) {
            var publisher = publisher(attempts, new IllegalStateException(), 0);
            assertThrows(IllegalArgumentException.class, () -> publisher.publish(event));
        }
        assertTrue(sends.isEmpty());
    }

    @Test
    void successStopsRetrying() throws Exception {
        var publisher = publisher(5, new IllegalStateException("transient"), 2);
        publisher.publish(event);
        assertEquals(3, sends.size());
    }

    @Test
    void interruptedSendRestoresInterruptFlagAndStopsRetrying() throws Exception {
        var publisher = publisher(5, new InterruptedException("interrupted"), 5);
        try {
            assertThrows(MQMessageSendException.class, () -> publisher.publish(event));
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(1, sends.size());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void fatalErrorsAreNotRetried() throws Exception {
        var error = new AssertionError("fatal");
        var publisher = publisher(5, error, 5);
        assertSame(error, assertThrows(AssertionError.class, () -> publisher.publish(event)));
        assertEquals(1, sends.size());
    }

    @Test
    void callbackFailureAfterSuccessfulSendDoesNotResend() throws Exception {
        var callbackFailure = new IllegalStateException("callback failed");
        var publisher = configure(new SparrowRocketMQPublisher() {
            public void after(Event event, String key) { throw callbackFailure; }
        }, 5, new IllegalStateException(), 0);
        assertSame(callbackFailure, assertThrows(IllegalStateException.class, () -> publisher.publish(event)));
        assertEquals(1, sends.size());
    }
}
