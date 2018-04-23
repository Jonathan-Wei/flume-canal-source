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

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.List;

class CanalClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalClient.class);
    private final CanalConf canalConf;

    private CanalConnector canalConnector;

    CanalClient(CanalConf canalConf) throws ServerUrlsFormatException {
        this.canalConf = canalConf;
        if (StringUtils.isNotEmpty(canalConf.getZkServers())) {
            this.canalConnector = getConnector(canalConf.getZkServers(), canalConf.getDestination(),
                    canalConf.getUsername(), canalConf.getPassword());
            LOGGER.trace(String.format("Cluster connector has been created. Zookeeper servers are %s, destination is %s",
                    canalConf.getZkServers(), canalConf.getDestination()));
        } else if (StringUtils.isNotEmpty(canalConf.getServerUrls())) {
            this.canalConnector = getConnector(CanalConf.convertUrlsToSocketAddressList(canalConf.getServerUrls()),
                    canalConf.getDestination(), canalConf.getUsername(), canalConf.getPassword());
            LOGGER.trace(String.format("Cluster connector has been created. Server urls are %s, destination is %s",
                    canalConf.getServerUrls(), canalConf.getDestination()));
        } else if (StringUtils.isNotEmpty(canalConf.getServerUrl())) {
            this.canalConnector = getConnector(CanalConf.convertUrlToSocketAddress(canalConf.getServerUrl()),
                    canalConf.getDestination(), canalConf.getUsername(), canalConf.getPassword());
        }
    }

    void start() {
        this.canalConnector.connect();
        String filterTables = Joiner.on(",").join(canalConf.getFilterTableList());
        this.canalConnector.subscribe(filterTables);
    }

    Message fetchRows(int batchSize) {
        Message message = this.canalConnector.getWithoutAck(batchSize);

        long batchId = message.getId();
        int size = message.getEntries().size();
        if (batchId == -1 || size == 0) {
            return null;
        } else {
            LOGGER.info("batch - {} data fetched successful, size is {}", batchId, size);
            return message;
        }
    }

    void ack(long batchId) {
        this.canalConnector.ack(batchId);
    }

    void rollback(long batchId) {
        this.canalConnector.rollback(batchId);
    }

    void stop() {
        this.canalConnector.disconnect();
    }

    private CanalConnector getConnector(String zkServers, String destination, String username, String password) {
        return CanalConnectors.newClusterConnector(zkServers, destination, username, password);
    }

    private CanalConnector getConnector(List<? extends SocketAddress> addresses, String destination,
                                        String username, String password) {
        return CanalConnectors.newClusterConnector(addresses, destination, username, password);
    }

    private CanalConnector getConnector(SocketAddress address, String destination, String username,
                                        String password) {
        return CanalConnectors.newSingleConnector(address, destination, username, password);
    }
}
