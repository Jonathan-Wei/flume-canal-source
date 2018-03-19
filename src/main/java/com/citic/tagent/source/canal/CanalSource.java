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
package com.citic.tagent.source.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.source.AbstractPollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CanalSource extends AbstractPollableSource
        implements Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanalSource.class);

    private CanalClient canalClient = null;
    private CanalConf canalConf = new CanalConf();

    @Override
    protected void doStart() throws FlumeException {
        LOGGER.trace("start...");

        try {
            this.canalClient = new CanalClient(canalConf);
            this.canalClient.start();
        } catch (ServerUrlsFormatException exception) {
            LOGGER.error(exception.getMessage(), exception);

            throw new FlumeException(exception);
        }
    }

    @Override
    protected void doStop() throws FlumeException {
        LOGGER.trace("stop...");
        this.canalClient.stop();
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

        // add
        canalConf.setTableToTopicMap(context.getString(CanalSourceConstants.TABLE_TO_TOPIC_MAP));

        LOGGER.debug("table_to_topic_map: {}",  canalConf.getTableToTopicMap().toString());


        if (!canalConf.isConnectionUrlValid()) {
            throw new ConfigurationException(String.format("\"%s\",\"%s\" AND \"%s\" at least one must be specified!",
                    CanalSourceConstants.ZOOKEEPER_SERVERS,
                    CanalSourceConstants.SERVER_URL,
                    CanalSourceConstants.SERVER_URLS));
        }
    }


    @Override
    protected Status doProcess() throws EventDeliveryException {
        try {
            Message message = canalClient.fetchRows(canalConf.getBatchSize());

            if (message != null) {
                try {
                    for (CanalEntry.Entry entry : message.getEntries()) {
                        getChannelProcessor().processEventBatch(
                                CanalEntryChannelEventConverter.convert(
                                        entry, canalConf
                                )
                        );
                    }
                } catch (Exception e) {
                    this.canalClient.rollback(message.getId());
                    LOGGER.warn(String.format("Exceptions occurs when channel processing batch events, message is %s", e.getMessage()));
                    return Status.BACKOFF;
                }

                this.canalClient.ack(message.getId());
                LOGGER.trace(String.format("Canal ack ok, batch id is %d", message.getId()));
                return Status.READY;
            } else {
                return Status.BACKOFF;
            }
        } catch (Exception e) {
            LOGGER.warn(String.format("Exceptions occurs when canal client fetching messages, message is %s", e.getMessage()));
            return Status.BACKOFF;
        }
    }
}
