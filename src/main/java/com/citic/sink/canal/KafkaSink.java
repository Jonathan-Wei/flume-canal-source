package com.citic.sink.canal;

import static com.citic.sink.canal.KafkaSinkConstants.ALERT_EVENT_DATA;
import static com.citic.sink.canal.KafkaSinkConstants.ALERT_EVENT_TOPIC;
import static com.citic.sink.canal.KafkaSinkConstants.ALERT_EXCEPTION;
import static com.citic.sink.canal.KafkaSinkConstants.ALERT_SCHEMA_NAME;
import static com.citic.sink.canal.KafkaSinkConstants.ALERT_TOPIC;
import static com.citic.sink.canal.KafkaSinkConstants.AVRO_KEY_SERIALIZER;
import static com.citic.sink.canal.KafkaSinkConstants.AVRO_VALUE_SERIAIZER;
import static com.citic.sink.canal.KafkaSinkConstants.BATCH_SIZE;
import static com.citic.sink.canal.KafkaSinkConstants.BOOTSTRAP_SERVERS_CONFIG;
import static com.citic.sink.canal.KafkaSinkConstants.BROKER_LIST_FLUME_KEY;
import static com.citic.sink.canal.KafkaSinkConstants.COUNT_MONITOR_INTERVAL;
import static com.citic.sink.canal.KafkaSinkConstants.DEFAULT_ACKS;
import static com.citic.sink.canal.KafkaSinkConstants.DEFAULT_BATCH_SIZE;
import static com.citic.sink.canal.KafkaSinkConstants.DEFAULT_COUNT_MONITOR_INTERVAL;
import static com.citic.sink.canal.KafkaSinkConstants.DEFAULT_KEY_SERIALIZER;
import static com.citic.sink.canal.KafkaSinkConstants.DEFAULT_TOPIC;
import static com.citic.sink.canal.KafkaSinkConstants.DEFAULT_VALUE_SERIAIZER;
import static com.citic.sink.canal.KafkaSinkConstants.KAFKA_PRODUCER_PREFIX;
import static com.citic.sink.canal.KafkaSinkConstants.KEY_HEADER;
import static com.citic.sink.canal.KafkaSinkConstants.KEY_SERIALIZER_KEY;
import static com.citic.sink.canal.KafkaSinkConstants.MESSAGE_SERIALIZER_KEY;
import static com.citic.sink.canal.KafkaSinkConstants.OLD_BATCH_SIZE;
import static com.citic.sink.canal.KafkaSinkConstants.REQUIRED_ACKS_FLUME_KEY;
import static com.citic.sink.canal.KafkaSinkConstants.SCHEMA_NAME;
import static com.citic.sink.canal.KafkaSinkConstants.SCHEMA_REGISTRY_URL_NAME;
import static com.citic.sink.canal.KafkaSinkConstants.SEND_ERROR_FILE_DEFAULT;
import static com.citic.sink.canal.KafkaSinkConstants.TOPIC_CONFIG;

import com.citic.helper.SchemaCache;
import com.citic.helper.Utility;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.LogPrivacyUtil;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 基于 Flume 1.8 版本官方 kafka sink 修改

/**
 * A Flume Sink that can publish messages to Kafka. This is a general implementation that can be
 * used with any Flume agent and a channel. The message can be any event and the key is a string
 * that we read from the header For use of partitioning, use an interceptor to generate a header
 * with the partition key
 * <p/>
 * Mandatory properties are: brokerList -- can be a partial list, but at least 2 are recommended for
 * HA
 * <p/>
 * <p/>
 * however, any property starting with "kafka." will be passed along to the Kafka producer Read the
 * Kafka producer documentation to see which configurations can be used
 * <p/>
 * Optional properties topic - there's a default, and also - this can be in the event header if you
 * need to support events with different topics batchSize - how many messages to process in one
 * batch. Larger batches improve throughput while adding latency. requiredAcks -- 0 (unsafe), 1
 * (accepted by at least one broker, default), -1 (accepted by all brokers) useFlumeEventFormat -
 * preserves event headers when serializing onto Kafka
 * <p/>
 * header properties (per event): topic key
 */

@javax.annotation.concurrent.NotThreadSafe
public class KafkaSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);

    private final Properties kafkaProps = new Properties();
    private KafkaProducer<Object, Object> producer;

    private String topic;
    private int batchSize;
    private List<Future<RecordMetadata>> kafkaFutures;
    private KafkaSinkCounter counter;
    private boolean useAvroEventFormat;
    private String partitionHeader = null;
    private Integer staticPartitionId = null;
    private boolean allowTopicOverride;
    private String topicHeader = null;
    private File kafkaSendErrorFile;
    private SendCountMonitor sendCountMonitor;
    private int countMonitorInterval;


    //For testing
    public String getTopic() {
        return topic;
    }

    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event = null;
        String eventTopic = null;
        String eventKey = null;

        try {
            transaction = channel.getTransaction();
            transaction.begin();

            kafkaFutures.clear();
            long batchStartTime = System.nanoTime();
            long processedEvents = 0;
            for (; processedEvents < batchSize; processedEvents += 1) {
                event = channel.take();

                if (event == null) {
                    // no events available in channel
                    if (processedEvents == 0) {
                        result = Status.BACKOFF;
                        counter.incrementBatchEmptyCount();
                    } else {
                        counter.incrementBatchUnderflowCount();
                    }
                    break;
                }

                Object eventBody = event.getBody();
                Map<String, String> headers = event.getHeaders();

                if (allowTopicOverride) {
                    eventTopic = headers.get(topicHeader);
                    if (eventTopic == null) {
                        eventTopic = BucketPath.escapeString(topic, event.getHeaders());
                        logger.debug("{} was set to true but header {} was null. Producing to {}"
                                + " topic instead.",
                            new Object[]{KafkaSinkConstants.ALLOW_TOPIC_OVERRIDE_HEADER,
                                topicHeader, eventTopic});
                    }
                } else {
                    eventTopic = topic;
                }

                eventKey = headers.get(KEY_HEADER);
                if (logger.isTraceEnabled()) {
                    logger.trace("{Event} " + eventTopic + " : " + eventKey);
                }
                logger.debug("event #{}", processedEvents);

                // create a message and add to buffer
                long startTime = System.currentTimeMillis();

                Integer partitionId = null;
                Object dataRecord = null;
                ProducerRecord<Object, Object> record;
                try {
                    if (useAvroEventFormat) {
                        dataRecord = serializeAvroEvent(event, headers.get(SCHEMA_NAME));
                    } else {
                        dataRecord = serializeJsonEvent(event);
                    }

                    if (staticPartitionId != null) {
                        partitionId = staticPartitionId;
                    }
                    //Allow a specified header to override a static ID
                    if (partitionHeader != null) {
                        String headerVal = event.getHeaders().get(partitionHeader);
                        if (headerVal != null) {
                            partitionId = Integer.parseInt(headerVal);
                        }
                    }
                    if (partitionId != null) {
                        record = new ProducerRecord<>(eventTopic, partitionId, eventKey,
                            dataRecord);
                    } else {
                        record = new ProducerRecord<>(eventTopic, eventKey,
                            dataRecord);
                    }
                    kafkaFutures
                        .add(producer.send(record, new SinkCallback(startTime, dataRecord, this)));
                } catch (NumberFormatException ex) {
                    throw new EventDeliveryException("Non integer partition id specified", ex);
                } catch (Exception ex) {
                    // N.B. The producer.send() method throws all sorts of RuntimeExceptions
                    // Catching Exception here to wrap them neatly in an EventDeliveryException
                    // which is what our consumers will expect
                    // 如果是avro 格式,对异常进行处理
                    if (useAvroEventFormat) {
                        logger.error("Could not send event", ex);
                        handleErrorData(dataRecord);
                        record = buildAlertInfoRecord(eventTopic, ex.getMessage(), dataRecord);
                        kafkaFutures.add(producer.send(record));
                    } else {
                        throw new EventDeliveryException("Could not send event", ex);
                    }
                }

            }

            //Prevent linger.ms from holding the batch
            producer.flush();

            // publish batch and commit.
            if (processedEvents > 0) {
                for (Future<RecordMetadata> future : kafkaFutures) {
                    future.get();
                }
                long endTime = System.nanoTime();
                counter.addToKafkaEventSendTimer((endTime - batchStartTime) / (1000 * 1000));
                counter.addToEventDrainSuccessCount((long) kafkaFutures.size());
            }

            transaction.commit();

        } catch (RuntimeException ex) {
            handleException(transaction, ex);
        } catch (Exception ex) {
            handleException(transaction, ex);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }

        return result;
    }

    private void handleException(Transaction transaction, Exception ex)
        throws EventDeliveryException {
        logger.error("Failed to publish events", ex);
        if (transaction != null) {
            try {
                kafkaFutures.clear();
                transaction.rollback();
                counter.incrementRollbackCount();
            } catch (Exception e) {
                logger.error("Transaction rollback failed", e);
                throw Throwables.propagate(e);
            }
        }
        String errorMsg = "Failed to publish events";
        throw new EventDeliveryException(errorMsg, ex);
    }


    @Override
    public synchronized void start() {
        // instantiate the producer
        producer = new KafkaProducer<>(kafkaProps);
        counter.start();
        super.start();

        if (sendCountMonitor == null) {
            sendCountMonitor = new SendCountMonitor(producer, kafkaFutures,
                useAvroEventFormat, countMonitorInterval);
        }
        sendCountMonitor.start();
    }

    @Override
    public synchronized void stop() {
        // 先发送统计数据再关闭
        sendCountMonitor.stop();

        producer.close();
        counter.stop();
        logger.info("Kafka Sink {} stopped. Metrics: {}", getName(), counter);
        super.stop();
    }


    /**
     * We configure the sink and generate properties for the Kafka Producer. Kafka producer
     * properties is generated as follows: 1. We generate a properties object with some static
     * defaults that can be overridden by Sink configuration 2. We add the configuration users added
     * for Kafka (parameters starting with .kafka. and must be valid Kafka Producer properties 3. We
     * add the sink's documented parameters which can override other properties
     */
    @Override
    public void configure(Context context) {
        translateOldProps(context);

        String topicStr = context.getString(TOPIC_CONFIG);
        if (topicStr == null || topicStr.isEmpty()) {
            topicStr = DEFAULT_TOPIC;
            logger.warn("Topic was not specified. Using {} as the topic.", topicStr);
        } else {
            logger.info("Using the static topic {}. This may be overridden by event headers",
                topicStr);
        }

        topic = topicStr;

        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);

        if (logger.isDebugEnabled()) {
            logger.debug("Using batch size: {}", batchSize);
        }

        useAvroEventFormat = context.getBoolean(KafkaSinkConstants.AVRO_EVENT,
            KafkaSinkConstants.DEFAULT_AVRO_EVENT);

        partitionHeader = context.getString(KafkaSinkConstants.PARTITION_HEADER_NAME);
        staticPartitionId = context.getInteger(KafkaSinkConstants.STATIC_PARTITION_CONF);

        allowTopicOverride = context.getBoolean(KafkaSinkConstants.ALLOW_TOPIC_OVERRIDE_HEADER,
            KafkaSinkConstants.DEFAULT_ALLOW_TOPIC_OVERRIDE_HEADER);

        topicHeader = context.getString(KafkaSinkConstants.TOPIC_OVERRIDE_HEADER,
            KafkaSinkConstants.DEFAULT_TOPIC_OVERRIDE_HEADER);

        countMonitorInterval = context
            .getInteger(COUNT_MONITOR_INTERVAL, DEFAULT_COUNT_MONITOR_INTERVAL);

        if (logger.isDebugEnabled()) {
            logger.debug(KafkaSinkConstants.AVRO_EVENT + " set to: {}", useAvroEventFormat);
        }

        kafkaFutures = new LinkedList<>();
        String bootStrapServers = context.getString(BOOTSTRAP_SERVERS_CONFIG);

        if (bootStrapServers == null || bootStrapServers.isEmpty()) {
            throw new ConfigurationException("Bootstrap Servers must be specified");
        }

        setProducerProps(context, bootStrapServers);

        if (logger.isDebugEnabled() && LogPrivacyUtil.allowLogPrintConfig()) {
            logger.debug("Kafka producer properties: {}", kafkaProps);
        }

        if (counter == null) {
            counter = new KafkaSinkCounter(getName());
        }

        if (kafkaSendErrorFile == null) {
            String fileName = context
                .getString(KafkaSinkConstants.SEND_ERROR_FILE, SEND_ERROR_FILE_DEFAULT);
            kafkaSendErrorFile = new File(fileName);
        }
    }

    private void translateOldProps(Context ctx) {
        if (!(ctx.containsKey(TOPIC_CONFIG))) {
            ctx.put(TOPIC_CONFIG, ctx.getString("topic"));
            logger.warn("{} is deprecated. Please use the parameter {}", "topic", TOPIC_CONFIG);
        }

        //Broker List
        // If there is no value we need to check and set the old param and log a warning message
        if (!(ctx.containsKey(BOOTSTRAP_SERVERS_CONFIG))) {
            String brokerList = ctx.getString(BROKER_LIST_FLUME_KEY);
            if (brokerList == null || brokerList.isEmpty()) {
                throw new ConfigurationException("Bootstrap Servers must be specified");
            } else {
                ctx.put(BOOTSTRAP_SERVERS_CONFIG, brokerList);
                logger.warn("{} is deprecated. Please use the parameter {}",
                    BROKER_LIST_FLUME_KEY, BOOTSTRAP_SERVERS_CONFIG);
            }
        }

        //batch Size
        if (!(ctx.containsKey(BATCH_SIZE))) {
            String oldBatchSize = ctx.getString(OLD_BATCH_SIZE);
            if (oldBatchSize != null && !oldBatchSize.isEmpty()) {
                ctx.put(BATCH_SIZE, oldBatchSize);
                logger.warn("{} is deprecated. Please use the parameter {}", OLD_BATCH_SIZE,
                    BATCH_SIZE);
            }
        }

        // Acks
        if (!(ctx.containsKey(KAFKA_PRODUCER_PREFIX + ProducerConfig.ACKS_CONFIG))) {
            String requiredKey = ctx.getString(
                KafkaSinkConstants.REQUIRED_ACKS_FLUME_KEY);
            if (!(requiredKey == null) && !(requiredKey.isEmpty())) {
                ctx.put(KAFKA_PRODUCER_PREFIX + ProducerConfig.ACKS_CONFIG, requiredKey);
                logger
                    .warn("{} is deprecated. Please use the parameter {}", REQUIRED_ACKS_FLUME_KEY,
                        KAFKA_PRODUCER_PREFIX + ProducerConfig.ACKS_CONFIG);
            }
        }

        if (ctx.containsKey(KEY_SERIALIZER_KEY)) {
            logger.warn(
                "{} is deprecated. Flume now uses the latest Kafka producer which implements "
                    + "a different interface for serializers. Please use the parameter {}",
                KEY_SERIALIZER_KEY,
                KAFKA_PRODUCER_PREFIX + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        }

        if (ctx.containsKey(MESSAGE_SERIALIZER_KEY)) {
            logger.warn(
                "{} is deprecated. Flume now uses the latest Kafka producer which implements "
                    + "a different interface for serializers. Please use the parameter {}",
                MESSAGE_SERIALIZER_KEY,
                KAFKA_PRODUCER_PREFIX + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        }
    }

    private void setProducerProps(Context context, String bootStrapServers) {
        kafkaProps.clear();
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, DEFAULT_ACKS);

        boolean useAvro = context.getBoolean(KafkaSinkConstants.AVRO_EVENT,
            KafkaSinkConstants.DEFAULT_AVRO_EVENT);

        if (useAvro) {
            //Defaults overridden based on config
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AVRO_KEY_SERIALIZER);
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AVRO_VALUE_SERIAIZER);

            String registryUrl = context.getString(KafkaSinkConstants.SCHEMA_REGISTRY_URL);
            kafkaProps.put(SCHEMA_REGISTRY_URL_NAME, registryUrl);
        } else {
            //Defaults overridden based on config
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER);
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERIAIZER);
        }

        kafkaProps.putAll(context.getSubProperties(KAFKA_PRODUCER_PREFIX));
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    }

    protected Properties getKafkaProps() {
        return kafkaProps;
    }

    private GenericRecord serializeAvroEvent(Event event, String schemaName) throws IOException {
        byte[] bytes = event.getBody();
        Schema schema = SchemaCache.getSchemaSink(schemaName);

        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        return recordInjection.invert(bytes).get();
    }


    private byte[] serializeJsonEvent(Event event) throws IOException {
        return event.getBody();
    }

    /*
    * 对出错的数据进行处理，并 count 错误日志行数
    * */
    private void handleErrorData(Object dataRecord) {
        if (dataRecord == null) {
            return;
        }
        try {
            String jsonString;
            if (useAvroEventFormat) {
                jsonString = Utility.avroToJson((GenericRecord) dataRecord);
            } else {
                jsonString = new String((byte[]) dataRecord, Charset.forName("UTF-8"));
            }
            Files.append(jsonString + "\n", kafkaSendErrorFile, Charsets.UTF_8);
        } catch (IOException e) {
            logger.error("Error when write message to error file", e);
        }
    }

    /*
     * 构建异常告警数据
     * */
    private ProducerRecord<Object, Object> buildAlertInfoRecord(String eventTopic,
        String exceptionInfo, Object dataRecord) {
        List<String> fieldList = Lists
            .newArrayList(ALERT_EVENT_TOPIC, ALERT_EXCEPTION, ALERT_EVENT_DATA);
        Schema schema = SchemaCache.getSchema(fieldList, ALERT_SCHEMA_NAME);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put(ALERT_EVENT_TOPIC, eventTopic);
        avroRecord.put(ALERT_EXCEPTION, exceptionInfo);
        avroRecord.put(ALERT_EVENT_DATA, Utility.avroToJson((GenericRecord) dataRecord));
        return new ProducerRecord<>(ALERT_TOPIC, avroRecord);
    }

    private static class SinkCallback implements Callback {

        private final long startTime;
        private final Object record;
        private final KafkaSink kafkaSink;

        SinkCallback(long startTime, Object record, KafkaSink kafkaSink) {
            this.record = record;
            this.startTime = startTime;
            this.kafkaSink = kafkaSink;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                logger.debug("Error sending message to Kafka {} ", exception.getMessage());
                kafkaSink.handleErrorData(this.record);
            }

            if (logger.isDebugEnabled()) {
                long eventElapsedTime = System.currentTimeMillis() - startTime;
                if (metadata != null) {
                    logger.debug("Acked message partition:{} ofset:{}", metadata.partition(),
                        metadata.offset());
                }
                logger.debug("Elapsed time for send: {}", eventElapsedTime);
            }
        }
    }


}


