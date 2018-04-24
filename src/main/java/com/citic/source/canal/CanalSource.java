/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.citic.source.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.collect.Lists;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.source.AbstractPollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.citic.source.canal.CanalSourceConstants.*;

public class CanalSource extends AbstractPollableSource
        implements Configurable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalSource.class);

    private CanalClient canalClient = null;
    private CanalConf canalConf;

    private final List<Event> eventsAll = Lists.newArrayList();

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
        canalConf.setPassword(context.getString(PASSWORD, DEFAULT_PASSWORD));
        canalConf.setBatchSize(context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE));
        canalConf.setTableToTopicMap(context.getString(TABLE_TO_TOPIC_MAP));
        canalConf.setTableFieldsFilter(context.getString(TABLE_FIELDS_FILTER));
        
        canalConf.setShutdownFlowCounter(context.getBoolean(SHUTDOWN_FLOW_COUNTER, DEFAULT_SHUTDOWN_FLOW_COUNTER));
    }

    @Override
    protected void doConfigure(Context context) throws FlumeException {
        LOGGER.debug("configure...");
        // 判断序列话格式
        boolean useAvro = context.getBoolean(USE_AVRO, true);
        canalConf = new CanalConf();

        setCanalConf(context);
        // TABLE_TO_TOPIC_MAP 配置不能为空
        if (canalConf.getTableToTopicMap() == null || canalConf.getTableToTopicMap().isEmpty()){
            throw new ConfigurationException(String.format("%s cannot be empty or null",
                    TABLE_TO_TOPIC_MAP));
        }

        if (!canalConf.isConnectionUrlValid()) {
            throw new ConfigurationException(String.format("\"%s\",\"%s\" AND \"%s\" at least one must be specified!",
                    ZOOKEEPER_SERVERS,
                    SERVER_URL,
                    SERVER_URLS));
        }

        if (sourceCounter == null)
            sourceCounter = new org.apache.flume.instrumentation.SourceCounter(getName());

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

        if (message == null)
            return Status.BACKOFF;

        for (CanalEntry.Entry entry : message.getEntries()) {
            List<Event> events = entryConverter.convert(entry, canalConf);
            eventsAll.addAll(events);
        }

        sourceCounter.addToEventReceivedCount(eventsAll.size());
        sourceCounter.incrementAppendBatchReceivedCount();

        try {
            getChannelProcessor().processEventBatch(eventsAll);
        } catch (Exception e) {
            this.canalClient.rollback(message.getId());
            LOGGER.warn("Exceptions occurs when channel processing batch events, message is {}",
                    e.getMessage(), e);
            
            eventsAll.clear();
            return Status.BACKOFF;
        }

        this.canalClient.ack(message.getId());
        sourceCounter.addToEventAcceptedCount(eventsAll.size());
        sourceCounter.incrementAppendBatchAcceptedCount();

        eventsAll.clear();
        LOGGER.debug("Canal ack ok, batch id is {}", message.getId());
        return Status.READY;
    }

    @Override
    protected void doStop() throws FlumeException {
        LOGGER.debug("stop...");
        this.canalClient.stop();
        sourceCounter.stop();

        LOGGER.info("" + "CanalSource source {} stopped. Metrics: {}", getName(), sourceCounter);
    }
}
