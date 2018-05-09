package com.citic.sink.canal;

import com.citic.helper.AgentCounter;
import com.citic.helper.FlowCounter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class SendCountMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendCountMonitor.class);
    private static final int AGENT_COUNTER_REPORT_INTERVAL = 5;


    private final int flowCountInterval;
    private final KafkaProducer<Object, Object> producer;
    private final List<Future<RecordMetadata>> kafkaFutures;
    private final boolean useAvro;

    private ScheduledExecutorService executorService;

    SendCountMonitor(KafkaProducer<Object, Object> producer,
        List<Future<RecordMetadata>> kafkaFutures,
        boolean useAvro,
        int flowCountInterval) {
        this.producer = producer;
        this.kafkaFutures = kafkaFutures;
        this.useAvro = useAvro;
        this.flowCountInterval = flowCountInterval;
    }

    void start() {
        // 进程检查时间间隔
        executorService = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("send-count-%d")
                .build());
        // 分两个线程单独监控
        executorService.scheduleAtFixedRate(new SendFlowCounterRunnable(), 0,
            flowCountInterval, TimeUnit.MINUTES);

        executorService.scheduleAtFixedRate(new SendAgentCounterRunnable(), 0,
            AGENT_COUNTER_REPORT_INTERVAL, TimeUnit.MINUTES);
    }

    void stop() {
        executorService.shutdown();
        try {
            while (!executorService.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                LOGGER.debug("Waiting for send count monitor to terminate");
            }
        } catch (InterruptedException e) {
            LOGGER.debug("Interrupted while send count monitor to terminate");
            Thread.currentThread().interrupt();
        }
    }

    private class SendFlowCounterRunnable implements Runnable {

        @Override
        public void run() {
            FlowCounter.flowCounterToEvents(useAvro).forEach(item -> {
                try {
                    kafkaFutures.add(producer.send(item));
                } catch (Exception ex) {
                    LOGGER.error("send flow counter to kafka error: ", ex);
                }
            });
        }
    }

    private class SendAgentCounterRunnable implements Runnable {

        @Override
        public void run() {
            AgentCounter.flowCounterToEvents(useAvro).forEach(item -> {
                try {
                    kafkaFutures.add(producer.send(item));
                } catch (Exception ex) {
                    LOGGER.error("send flow counter to kafka error: ", ex);
                }
            });
        }
    }
}
