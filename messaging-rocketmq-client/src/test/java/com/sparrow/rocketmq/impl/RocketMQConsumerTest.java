package com.sparrow.rocketmq.impl;

import com.sparrow.messaging.rocketmq.RocketMQConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RocketMQConsumerTest {
    @Test
    void defaultThreadCountConfiguresValidMinimumAndMaximum() {
        var consumer = new ExposedConsumer();
        consumer.setGroupName("review-test");
        consumer.setTopicConfig("topic:tag");
        consumer.setMessageListener((messages, context) -> null);
        var configured = (DefaultMQPushConsumer) consumer.create();
        assertEquals(20, configured.getConsumeThreadMin());
        assertEquals(20, configured.getConsumeThreadMax());
        configured.shutdown();
    }

    @Test
    void rejectsNonPositiveThreadCounts() {
        var consumer = new ExposedConsumer();
        consumer.setGroupName("review-test");
        consumer.setTopicConfig("topic:tag");
        consumer.setMessageListener((messages, context) -> null);
        consumer.setThreadCount(0);
        assertThrows(RuntimeException.class, consumer::create);
    }

    @Test
    void repeatedStartAndShutdownAreIdempotent() {
        var consumer = new ScriptedConsumer();
        var running = consumer.enqueue(false, false);
        consumer.shutdown();
        consumer.start();
        consumer.start();
        consumer.shutdown();
        consumer.shutdown();
        assertEquals(1, running.starts);
        assertEquals(1, running.stops);
        assertEquals(1, consumer.created.size());
    }

    @Test
    void configurationBeforeStartIsAppliedOnFirstStart() {
        var consumer = new ScriptedConsumer();
        var running = consumer.enqueue(false, false);
        consumer.setPullSize(4, 16);
        assertEquals(List.of(), consumer.created);
        consumer.start();
        assertEquals(List.of("4:16"), consumer.created);
        assertEquals(1, running.starts);
    }

    @Test
    void runningAdjustmentStartsReplacementAndCommitsNewConfiguration() {
        var consumer = new ScriptedConsumer();
        var old = consumer.enqueue(false, false);
        var replacement = consumer.enqueue(false, false);
        var restart = consumer.enqueue(false, false);
        consumer.start();
        consumer.setPullSize(4, 16);
        assertEquals(1, old.stops);
        assertEquals(1, replacement.starts);
        consumer.shutdown();
        consumer.start();
        assertEquals(List.of("1:0", "4:16", "4:16"), consumer.created);
        assertEquals(1, restart.starts);
    }

    @Test
    void failedAdjustmentRestoresOldConfigurationAndReportsFailure() {
        var consumer = new ScriptedConsumer();
        consumer.enqueue(false, false);
        var failed = consumer.enqueue(true, false);
        var restored = consumer.enqueue(false, false);
        consumer.start();
        assertThrows(RuntimeException.class, () -> consumer.setPullSize(4, 16));
        assertEquals(1, failed.stops);
        assertEquals(1, restored.starts);
        assertEquals(List.of("1:0", "4:16", "1:0"), consumer.created);
        consumer.start();
        assertEquals(3, consumer.created.size());
    }

    @Test
    void failedAdjustmentAndFailedRecoveryLeaveRestartPossibleAndKeepBothFailures() {
        var consumer = new ScriptedConsumer();
        consumer.enqueue(false, false);
        consumer.enqueue(true, false);
        var failedRecovery = consumer.enqueue(true, false);
        var next = consumer.enqueue(false, false);
        consumer.start();
        var failure = assertThrows(RuntimeException.class, () -> consumer.setPullSize(4, 16));
        assertTrue(failure.getSuppressed().length > 0 || (failure.getCause() != null && failure.getCause().getSuppressed().length > 0));
        assertEquals(1, failedRecovery.stops);
        consumer.start();
        assertEquals(1, next.starts);
        assertEquals(List.of("1:0", "4:16", "1:0", "1:0"), consumer.created);
    }

    @Test
    void failedFirstStartCleansCandidateAndAllowsRetry() {
        var consumer = new ScriptedConsumer();
        var failed = consumer.enqueue(true, false);
        var next = consumer.enqueue(false, false);
        assertThrows(RuntimeException.class, consumer::start);
        assertEquals(1, failed.stops);
        consumer.start();
        assertEquals(1, next.starts);
    }

    @Test
    void cleanupFailureDoesNotReplaceStartFailure() {
        var consumer = new ScriptedConsumer();
        consumer.enqueue(true, true);
        var failure = assertThrows(RuntimeException.class, consumer::start);
        assertTrue(failure.toString().contains("start") || failure.getCause().toString().contains("start"));
        assertTrue(failure.getSuppressed().length > 0 || failure.getCause().getSuppressed().length > 0);
    }

    @Test
    void concurrentStartCreatesOnlyOneRunningInstance() throws Exception {
        var consumer = new ScriptedConsumer();
        var running = consumer.enqueue(false, false);
        var gate = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(8)) {
            var tasks = new ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < 8; i++) {
                tasks.add(executor.submit(() -> { gate.await(); consumer.start(); return null; }));
            }
            gate.countDown();
            for (var task : tasks) { task.get(5, TimeUnit.SECONDS); }
        }
        assertEquals(1, consumer.created.size());
        assertEquals(1, running.starts);
        consumer.shutdown();
    }

    @Test
    void concurrentAdjustmentAndShutdownLeaveConfigurationRestartable() throws Exception {
        var consumer = new ScriptedConsumer();
        var original = consumer.enqueue(false, false);
        var second = consumer.enqueue(false, false);
        var third = consumer.enqueue(false, false);
        consumer.start();
        var gate = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var adjustment = executor.submit(() -> { gate.await(); consumer.setPullSize(4, 16); return null; });
            var shutdown = executor.submit(() -> { gate.await(); consumer.shutdown(); return null; });
            gate.countDown();
            adjustment.get(5, TimeUnit.SECONDS);
            shutdown.get(5, TimeUnit.SECONDS);
        }
        consumer.start();
        assertEquals("4:16", consumer.created.getLast());
        assertEquals(1, original.stops);
        assertEquals(1, original.starts + second.starts + third.starts - original.stops - second.stops - third.stops);
        consumer.shutdown();
    }

    @Test
    void zeroSizesKeepSdkDefaultsAndRemainRestartable() {
        var consumer = new ScriptedConsumer();
        var running = consumer.enqueue(false, false);
        var replacement = consumer.enqueue(false, false);
        consumer.start();
        consumer.setPullSize(0, 0);
        assertEquals(1, running.stops);
        assertEquals(1, replacement.starts);
        assertEquals(List.of("1:0", "0:0"), consumer.created);
        consumer.shutdown();
    }

    @Test
    void failureToStopOldConsumerDoesNotCreateAnotherInstance() {
        var consumer = new ScriptedConsumer();
        consumer.enqueue(false, true);
        consumer.start();
        assertThrows(IllegalStateException.class, () -> consumer.setPullSize(4, 16));
        consumer.start();
        assertEquals(1, consumer.created.size());
    }

    private static class ExposedConsumer extends RocketMQConsumer {
        MQPushConsumer create() { return createConsumer(1, 0); }
    }

    private static class ScriptedConsumer extends RocketMQConsumer {
        final ArrayDeque<StubConsumer> queued = new ArrayDeque<>();
        final List<String> created = new ArrayList<>();
        StubConsumer enqueue(boolean failStart, boolean failStop) {
            var stub = new StubConsumer(failStart, failStop);
            queued.add(stub);
            return stub;
        }
        protected MQPushConsumer createConsumer(Integer batch, Integer pull) {
            created.add(batch + ":" + pull);
            return queued.remove().proxy;
        }
    }

    private static class StubConsumer {
        int starts;
        int stops;
        final MQPushConsumer proxy;
        StubConsumer(boolean failStart, boolean failStop) {
            proxy = (MQPushConsumer) Proxy.newProxyInstance(MQPushConsumer.class.getClassLoader(), new Class[]{MQPushConsumer.class}, (p, method, args) -> {
                if (method.getName().equals("start")) {
                    starts++;
                    if (failStart) { throw new MQClientException("start failed", null); }
                } else if (method.getName().equals("shutdown")) {
                    stops++;
                    if (failStop) { throw new IllegalStateException("cleanup failed"); }
                }
                return null;
            });
        }
    }
}
