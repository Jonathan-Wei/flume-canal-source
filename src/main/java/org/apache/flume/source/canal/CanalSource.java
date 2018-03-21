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
package org.apache.flume.source.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.collect.Lists;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractPollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CanalSource extends AbstractPollableSource
        implements Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanalSource.class);

    private CanalClient canalClient = null;
    private CanalConf canalConf = new CanalConf();

    private SourceCounter sourceCounter;

    @Override
    protected void doStart() throws FlumeException {
        LOGGER.trace("start...");

        LOGGER.info("Object name: {}", this.getClass().getName().toString());

        try {
            this.canalClient = new CanalClient(canalConf);
            this.canalClient.start();
        } catch (ServerUrlsFormatException exception) {
            LOGGER.error(exception.getMessage(), exception);

            throw new FlumeException(exception);
        }

        sourceCounter.start();
    }

    @Override
    protected void doStop() throws FlumeException {
        LOGGER.trace("stop...");
        this.canalClient.stop();

        sourceCounter.stop();
        LOGGER.info("" +
                "CanalSource source {} stopped. Metrics: {}", getName(), sourceCounter);
    }

    @Override
    protected void doConfigure(Context context) throws FlumeException {
        LOGGER.trace("configure...");

        canalConf.setServerUrl(context.getString(CanalSourceConstants.SERVER_URL));
        canalConf.setServerUrls(context.getString(CanalSourceConstants.SERVER_URLS));
        canalConf.setZkServers(context.getString(CanalSourceConstants.ZOOKEEPER_SERVERS));
        canalConf.setDestination(context.getString(CanalSourceConstants.DESTINATION));
        canalConf.setUsername(context.getString(CanalSourceConstants.USERNAME,
                CanalSourceConstants.DEFAULT_USERNAME));
        canalConf.setPassword(context.getString(CanalSourceConstants.PASSWORD,
                CanalSourceConstants.DEFAULT_PASSWORD));
        canalConf.setFilter(context.getString(CanalSourceConstants.FILTER));
        canalConf.setBatchSize(context.getInteger(CanalSourceConstants.BATCH_SIZE,
                CanalSourceConstants.DEFAULT_BATCH_SIZE));
        canalConf.setOldDataRequired(context.getBoolean(CanalSourceConstants.OLD_DATA_REQUIRED,
                CanalSourceConstants.DEFAULT_OLD_DATA_REQUIRED));

        canalConf.setTableToTopicMap(context.getString(CanalSourceConstants.TABLE_TO_TOPIC_MAP));

        canalConf.setTableFieldsFilter(context.getString(CanalSourceConstants.TABLE_FIELDS_FILTER));

        LOGGER.debug("table_fields_table: {}",  canalConf.getTableFieldsFilter().toString());

        if (!canalConf.isConnectionUrlValid()) {
            throw new ConfigurationException(String.format("\"%s\",\"%s\" AND \"%s\" at least one must be specified!",
                    CanalSourceConstants.ZOOKEEPER_SERVERS,
                    CanalSourceConstants.SERVER_URL,
                    CanalSourceConstants.SERVER_URLS));
        }

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }


    @Override
    protected Status doProcess() {
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

        List<Event> eventsAll = Lists.newArrayList();

        for (CanalEntry.Entry entry : message.getEntries()) {
            List<Event> events = CanalEntryChannelEventConverter.convert(entry, canalConf);
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
            return Status.BACKOFF;
        }

        this.canalClient.ack(message.getId());
        sourceCounter.addToEventAcceptedCount(eventsAll.size());
        sourceCounter.incrementAppendBatchAcceptedCount();

        LOGGER.debug("Canal ack ok, batch id is {}", message.getId());
        return Status.READY;
    }

}