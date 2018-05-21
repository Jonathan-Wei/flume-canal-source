package com.citic.source.canal;

import static com.citic.source.canal.CanalSourceConstants.BATCH_SIZE;
import static com.citic.source.canal.CanalSourceConstants.DEFAULT_BATCH_SIZE;
import static com.citic.source.canal.CanalSourceConstants.DEFAULT_PSWD;
import static com.citic.source.canal.CanalSourceConstants.DEFAULT_SHUTDOWN_FLOW_COUNTER;
import static com.citic.source.canal.CanalSourceConstants.DEFAULT_USERNAME;
import static com.citic.source.canal.CanalSourceConstants.DEFAULT_WRITE_SQL_TO_DATA;
import static com.citic.source.canal.CanalSourceConstants.DESTINATION;
import static com.citic.source.canal.CanalSourceConstants.IP_INTERFACE;
import static com.citic.source.canal.CanalSourceConstants.MIN_BATCH_SIZE;
import static com.citic.source.canal.CanalSourceConstants.PSWD;
import static com.citic.source.canal.CanalSourceConstants.SERVER_URL;
import static com.citic.source.canal.CanalSourceConstants.SERVER_URLS;
import static com.citic.source.canal.CanalSourceConstants.SHUTDOWN_FLOW_COUNTER;
import static com.citic.source.canal.CanalSourceConstants.TABLE_FIELDS_FILTER;
import static com.citic.source.canal.CanalSourceConstants.TABLE_TO_TOPIC_MAP;
import static com.citic.source.canal.CanalSourceConstants.USERNAME;
import static com.citic.source.canal.CanalSourceConstants.USE_AVRO;
import static com.citic.source.canal.CanalSourceConstants.WRITE_SQL_TO_DATA;
import static com.citic.source.canal.CanalSourceConstants.ZOOKEEPER_SERVERS;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.source.AbstractPollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CanalSource extends AbstractPollableSource
    implements Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanalSource.class);
    private final List<Event> eventsAll = Lists.newArrayList();
    private CanalClient canalClient = null;
    private CanalConf canalConf;
    private org.apache.flume.instrumentation.SourceCounter sourceCounter;
    private EntryConverter entryConverter;

    /*
     * 获取配置
     * */
    private void setCanalConf(Context context) {
        canalConf.setIpInterface(context.getString(IP_INTERFACE));
        canalConf.setServerUrl(context.getString(SERVER_URL));
        canalConf.setServerUrls(context.getString(SERVER_URLS));
        canalConf.setZkServers(context.getString(ZOOKEEPER_SERVERS));
        canalConf.setDestination(context.getString(DESTINATION));
        canalConf.setUsername(context.getString(USERNAME, DEFAULT_USERNAME));
        canalConf.setPassword(context.getString(PSWD, DEFAULT_PSWD));
        canalConf.setBatchSize(context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE));
        canalConf.setTableToTopicMap(context.getString(TABLE_TO_TOPIC_MAP));
        canalConf.setTableFieldsFilter(context.getString(TABLE_FIELDS_FILTER));

        canalConf.setShutdownFlowCounter(
            context.getBoolean(SHUTDOWN_FLOW_COUNTER, DEFAULT_SHUTDOWN_FLOW_COUNTER));
        canalConf
            .setWriteSqlToData(context.getBoolean(WRITE_SQL_TO_DATA, DEFAULT_WRITE_SQL_TO_DATA));
    }

    @Override
    protected void doConfigure(Context context) throws FlumeException {
        LOGGER.debug("configure...");
        // 判断序列话格式
        canalConf = new CanalConf();

        setCanalConf(context);
        // TABLE_TO_TOPIC_MAP 配置不能为空
        if (canalConf.getTableToTopicMap() == null || canalConf.getTableToTopicMap().isEmpty()) {
            throw new ConfigurationException(String.format("%s cannot be empty or null",
                TABLE_TO_TOPIC_MAP));
        }

        if (!canalConf.isConnectionUrlValid()) {
            throw new ConfigurationException(
                String.format("\"%s\",\"%s\" AND \"%s\" at least one must be specified!",
                    ZOOKEEPER_SERVERS,
                    SERVER_URL,
                    SERVER_URLS));
        }

        if (sourceCounter == null) {
            sourceCounter = new org.apache.flume.instrumentation.SourceCounter(getName());
        }

        boolean useAvro = context.getBoolean(USE_AVRO, true);
        entryConverter = new EntryConverter(useAvro, canalConf);
    }

    @Override
    protected void doStart() throws FlumeException {
        LOGGER.debug("start...");
        try {
            this.canalClient = new CanalClient(canalConf);
            this.canalClient.start();
        } catch (IllegalArgumentException exception) {
            LOGGER.error(exception.getMessage(), exception);

            throw new FlumeException(exception);
        }

        sourceCounter.start();
    }

    @Override
    protected Status doProcess() {
        LOGGER.debug("doProcess...");
        Message message;
        try {
            message = canalClient.fetchRows(canalConf.getBatchSize());
        } catch (Exception e) {
            LOGGER.error("Exceptions occurs when canal client fetching messages, message is {}",
                e.getMessage(), e);
            return Status.BACKOFF;
        }

        if (message == null) {
            return Status.BACKOFF;
        }

        for (CanalEntry.Entry entry : message.getEntries()) {
            List<Event> events = entryConverter.convert(entry, canalConf);
            eventsAll.addAll(events);
        }

        sourceCounter.addToEventReceivedCount(eventsAll.size());
        sourceCounter.incrementAppendBatchReceivedCount();

        getChannelProcessor().processEventBatch(eventsAll);

        try {
            getChannelProcessor().processEventBatch(eventsAll);
        } catch (ChannelException ex) {
            return handleProcessError(ex, message);
        } catch (Exception e) {
            //TODO: 考虑动态增加 batch size
            int reduceBatchSize = Math.max(canalConf.getBatchSize() - 96, MIN_BATCH_SIZE);
            canalConf.setBatchSize(reduceBatchSize);
            return handleProcessError(e, message);
        }

        this.canalClient.ack(message.getId());
        sourceCounter.addToEventAcceptedCount(eventsAll.size());
        sourceCounter.incrementAppendBatchAcceptedCount();

        eventsAll.clear();
        LOGGER.debug("Canal ack ok, batch id is {}", message.getId());
        return Status.READY;
    }

    private Status handleProcessError(Exception e, Message message) {
        this.canalClient.rollback(message.getId());
        LOGGER.warn("Exceptions occurs when channel processing batch events, message is {}",
            e.getMessage(), e);

        eventsAll.clear();
        LOGGER.warn("Current batch size: {}", canalConf.getBatchSize());
        return Status.BACKOFF;
    }

    @Override
    protected void doStop() throws FlumeException {
        LOGGER.debug("stop...");
        this.canalClient.stop();
        sourceCounter.stop();

        LOGGER.info("" + "CanalSource source {} stopped. Metrics: {}", getName(), sourceCounter);
    }
}
