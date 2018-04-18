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

import com.citic.helper.RegexHashMap;
import com.citic.helper.Utility;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;

class CanalConf {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalConf.class);
    private String IPAddress;
    private String zkServers;
    private String destination;
    private String username;
    private String password;
    private int batchSize;
    private String serverUrl;
    private String serverUrls;

    private String tableToTopicMap;
    private String tableFieldsFilter;

    // db.table -> topic
    private Map<String, String> tableToTopicRegexMap = new RegexHashMap<>();
    private List<String> filterTableList = Lists.newArrayList();


    void setTableFieldsFilter(String tableFieldsFilter){
        this.tableFieldsFilter = tableFieldsFilter;
    }

    String getTableFieldsFilter(){
        return this.tableFieldsFilter;
    }

    void setTableToTopicMap(String tableToTopicMap){
        this.tableToTopicMap = tableToTopicMap;
        splitTableToTopicMap(tableToTopicMap);
    }

    String getTableToTopicMap(){
        return this.tableToTopicMap;
    }

    String getFromDBIP() {
        // destination example: 192_168_2_24-3306
        return this.destination.replace("-", ":").replace("_", ".");
    }

    private void splitTableToTopicMap(String tableToTopicMap) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableToTopicMap), "tableToTopicMap cannot empty");
        Splitter.on(';')
                .omitEmptyStrings()
                .trimResults()
                .split(tableToTopicMap)
                .forEach(item ->{
                    String[] result =  item.split(":");

                    filterTableList.add(result[0].trim());
                    // db.table -> topic
                    this.tableToTopicRegexMap.put(result[0].trim(), result[1].trim());
                });
    }

    /*
    * 根据表名获取 topic
    * */
    String getTableTopic(String schemaTableName) {
        if (this.tableToTopicRegexMap != null)
            return this.tableToTopicRegexMap.getOrDefault(schemaTableName, CanalSourceConstants.DEFAULT_NOT_MAP_TOPIC);
        else
            return CanalSourceConstants.DEFAULT_NOT_MAP_TOPIC;
    }

    void setZkServers(String zkServers) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(zkServers), "zkServers cannot empty");
        this.zkServers = zkServers;
    }

    void setDestination(String destination) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(destination), "destination cannot empty");
        this.destination = destination;
    }

    String getIPAddress() { return IPAddress; };


    String getZkServers() { return zkServers; }

    String getDestination() { return destination;}

    String getUsername() { return username; }

    String getPassword() { return password;}

    int getBatchSize() { return batchSize; }

    String getServerUrl() { return serverUrl; }

    String getServerUrls() { return serverUrls; }

    void setUsername(String username) { this.username = username;}

    void setPassword(String password) { this.password = password;}

    void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    void setServerUrl(String serverUrl) { this.serverUrl = serverUrl; }

    void setIpInterface(String ipInterface) { IPAddress = Utility.getLocalIP(ipInterface); }

    void setServerUrls(String serverUrls) { this.serverUrls = serverUrls;}

    /*
    * 获取需要过滤的表列表
    * */
    String getTableFilter() {
        return Joiner.on(",").join(filterTableList);
    }

    boolean isConnectionUrlValid() {
        return !(Strings.isNullOrEmpty(this.zkServers)
                && Strings.isNullOrEmpty(this.serverUrl)
                && Strings.isNullOrEmpty(this.serverUrls));
    }

    static List<SocketAddress> convertUrlsToSocketAddressList(String serverUrls) throws
            ServerUrlsFormatException {
        List<SocketAddress> addresses = new ArrayList<>();
        if (StringUtils.isNotEmpty(serverUrls)) {
            for (String serverUrl : serverUrls.split(",")) {
                if (StringUtils.isNotEmpty(serverUrl)) {
                    try {
                        addresses.add(convertUrlToSocketAddress(serverUrl));
                    } catch (Exception exception) {
                        throw new ServerUrlsFormatException(String.format("The serverUrls are malformed. " +
                                "The ServerUrls : \"%s\" .", serverUrls), exception);
                    }
                }
            }
            return addresses;
        } else {
            return addresses;
        }
    }

    static SocketAddress convertUrlToSocketAddress(String serverUrl) throws ServerUrlsFormatException,
            NumberFormatException {
        String[] hostAndPort = serverUrl.split(":");
        if (hostAndPort.length == 2 && StringUtils.isNotEmpty(hostAndPort[1])) {
            int port  = Integer.parseInt(hostAndPort[1]);
            return new InetSocketAddress(hostAndPort[0], port);
        } else {
            throw new ServerUrlsFormatException(String.format("The serverUrl is malformed . The ServerUrl : \"%s\" .",
                    serverUrl));
        }
    }
}
