package com.citic.sink.canal;

import org.apache.kafka.clients.CommonClientConfigs;

public class KafkaSinkConstants {

    public static final String KAFKA_PREFIX = "kafka.";
    public static final String KAFKA_PRODUCER_PREFIX = KAFKA_PREFIX + "producer.";

    /* Properties */

    public static final String TOPIC_CONFIG = KAFKA_PREFIX + "topic";
    public static final String BATCH_SIZE = "flumeBatchSize";
    public static final String BOOTSTRAP_SERVERS_CONFIG =
        KAFKA_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    public static final String KEY_HEADER = "key";
    public static final String SCHEMA_NAME = "schema";
    public static final String DEFAULT_TOPIC_OVERRIDE_HEADER = "topic";

    public static final String TOPIC_OVERRIDE_HEADER = "topicHeader";
    public static final String SCHEMA_REGISTRY_URL_NAME = "schema.registry.url";


    public static final String COUNT_MONITOR_INTERVAL = "countMonitorInterval";
    public static final int DEFAULT_COUNT_MONITOR_INTERVAL = 5;


    public static final String SCHEMA_REGISTRY_URL = KAFKA_PREFIX + "registryUrl";
    public static final String SEND_ERROR_FILE = KAFKA_PREFIX + "sendErrorFile";
    public static final String SEND_ERROR_FILE_DEFAULT = "logs/send-error.log";


    public static final String ALLOW_TOPIC_OVERRIDE_HEADER = "allowTopicOverride";
    public static final boolean DEFAULT_ALLOW_TOPIC_OVERRIDE_HEADER = true;

    public static final String AVRO_EVENT = "useAvroEventFormat";
    public static final boolean DEFAULT_AVRO_EVENT = true;

    public static final String PARTITION_HEADER_NAME = "partitionIdHeader";
    public static final String STATIC_PARTITION_CONF = "defaultPartitionId";

    public static final String DEFAULT_KEY_SERIALIZER =
        "org.apache.kafka.common.serialization.StringSerializer";
    public static final String DEFAULT_VALUE_SERIAIZER =
        "org.apache.kafka.common.serialization.ByteArraySerializer";

    public static final String AVRO_KEY_SERIALIZER =
        "io.confluent.kafka.serializers.KafkaAvroSerializer";
    public static final String AVRO_VALUE_SERIAIZER =
        "io.confluent.kafka.serializers.KafkaAvroSerializer";

    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final String DEFAULT_TOPIC = "default-flume-topic";
    public static final String DEFAULT_ACKS = "1";

    /* Old Properties */

    /* Properties */
    public static final String OLD_BATCH_SIZE = "batchSize";
    public static final String MESSAGE_SERIALIZER_KEY = "serializer.class";
    public static final String KEY_SERIALIZER_KEY = "key.serializer.class";
    public static final String BROKER_LIST_FLUME_KEY = "brokerList";
    public static final String REQUIRED_ACKS_FLUME_KEY = "requiredAcks";


    public static final String ALERT_EVENT_TOPIC = "event_topic";
    public static final String ALERT_EXCEPTION = "exception";
    public static final String ALERT_EVENT_DATA = "event_data";
    public static final String ALERT_SCHEMA_NAME = "alter_info";
    public static final String ALERT_TOPIC = "avro_error_alert";

}
