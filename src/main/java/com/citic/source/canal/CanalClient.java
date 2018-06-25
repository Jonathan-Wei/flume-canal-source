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
import java.net.SocketAddress;
import java.util.List;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Canal client.
 */
public class CanalClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanalClient.class);
    private final CanalConf canalConf;

    private CanalConnector canalConnector;

    /**
     * Instantiates a new Canal client.
     *
     * @param canalConf the canal conf
     * @throws IllegalArgumentException the illegal argument exception
     */
    public CanalClient(CanalConf canalConf) throws InterruptedException {
        this.canalConf = canalConf;
        if (StringUtils.isNotEmpty(canalConf.getZkServers())) {
            this.canalConnector = getConnector(canalConf.getZkServers(), canalConf.getDestination(),
                canalConf.getUsername(), canalConf.getPassword());
            LOGGER.trace(
                "Cluster connector has been created. Zookeeper servers are {}, destination is {}",
                canalConf.getZkServers(), canalConf.getDestination());
        } else if (StringUtils.isNotEmpty(canalConf.getServerUrls())) {
            this.canalConnector = getConnector(
                CanalConf.convertUrlsToSocketAddressList(canalConf.getServerUrls()),
                canalConf.getDestination(), canalConf.getUsername(), canalConf.getPassword());
            LOGGER
                .trace("Cluster connector has been created. Server urls are {}, destination is {}",
                    canalConf.getServerUrls(), canalConf.getDestination());
        } else if (StringUtils.isNotEmpty(canalConf.getServerUrl())) {
            this.canalConnector = getConnector(
                CanalConf.convertUrlToSocketAddress(canalConf.getServerUrl()),
                canalConf.getDestination(), canalConf.getUsername(), canalConf.getPassword());
        }
    }

    /**
     * Start.
     */
    public void start() {
        this.canalConnector.connect();
        String filterTables = Joiner.on(",").join(canalConf.getFilterTableList());
        this.canalConnector.subscribe(filterTables);
    }

    /**
     * Fetch rows message.
     *
     * @param batchSize the batch size
     * @return the message
     */
    public Message fetchRows(int batchSize) {
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

    /**
     * Ack.
     *
     * @param batchId the batch id
     */
    public void ack(long batchId) {
        this.canalConnector.ack(batchId);
    }

    /**
     * Rollback.
     *
     * @param batchId the batch id
     */
    public void rollback(long batchId) {
        this.canalConnector.rollback(batchId);
    }

    /**
     * Stop.
     */
    public void stop() {
        this.canalConnector.disconnect();
    }

    private CanalConnector getZkConnector(String zkServers, String destination, String username,
        String password, int sleepMillis, int retryCount) throws InterruptedException {
        try {
            return CanalConnectors.newClusterConnector(zkServers, destination, username, password);
        } catch (ZkNoNodeException ex) {
            LOGGER.warn(ex.getLocalizedMessage());
            if (retryCount > 0) {
                Thread.sleep(sleepMillis);
                // 如果 canal 和 flume 同时启动，可能出现 canal 还没有在zookeeper注册注册成功的情况
                return getZkConnector(zkServers, destination, username, password,
                    sleepMillis + 1000,
                    retryCount - 1);
            } else {
                throw ex;
            }
        }
    }

    private CanalConnector getConnector(String zkServers, String destination, String username,
        String password) throws InterruptedException {
        return getZkConnector(zkServers, destination, username, password, 1000, 5);
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
