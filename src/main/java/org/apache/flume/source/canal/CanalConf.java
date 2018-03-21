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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;

public class CanalConf {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalConf.class);

    private String zkServers;
    private String destination;
    private String username;
    private String password;
    private int batchSize;
    private String serverUrl;
    private String serverUrls;
    private String filter;
    private Boolean oldDataRequired;
    private Map<String, String> tableToTopicMap;

    private Table<String, String, Boolean> tableFieldsFilter;

    /*
    * 设置表名与字段过滤对应 table
    * */
    public void setTableFieldsFilter(String tableFieldsFilter) {
        // schema.table_name:field_name,field_name;schema.table_name:field_name,field_name
        this.tableFieldsFilter = HashBasedTable.create();
        Splitter.on(';')
            .omitEmptyStrings()
            .trimResults()
            .withKeyValueSeparator(":")
            .split(tableFieldsFilter)
            .forEach((k, v) -> {
                Iterable<String> fieldList =
                        Splitter.on(",").omitEmptyStrings().trimResults().split(v);
                for (String field : fieldList) {
                    this.tableFieldsFilter.put(k, field, true);
                }
            });
    }

    /*
    * 判断表名，字段是否在过滤列表中
    * */
    public boolean isFiledInTable(String schemaTableName, String fieldName) {
        if (this.tableFieldsFilter != null) {
            return this.tableFieldsFilter.contains(schemaTableName, fieldName);
        } else {
            return false;
        }
    }

    /*
    * 设置表名和 topic 对应 map
    * */
    public void setTableToTopicMap(String tableToTopicMap) {
        // test.test:test123;test.test1:test234
        this.tableToTopicMap  = Splitter.on(';')
                .omitEmptyStrings()
                .trimResults()
                .withKeyValueSeparator(":")
                .split(tableToTopicMap);
    }

    /*
    * 根据表名获取 topic
    * */
    public String getTableTopic(String schemaTableName) {
        if (this.tableToTopicMap != null)
            return this.tableToTopicMap.getOrDefault(schemaTableName, CanalSourceConstants.DEFAULT_NOT_MAP_TOPIC);
        else
            return CanalSourceConstants.DEFAULT_NOT_MAP_TOPIC;
    }

    public Table<String, String, Boolean> getTableFieldsFilter() {
        return this.tableFieldsFilter;
    }

    public String getZkServers() {
        return zkServers;
    }

    public void setZkServers(String zkServers) {
        this.zkServers = zkServers;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getServerUrl() {
        return serverUrl;
    }

    public void setServerUrl(String serverUrl) {
        this.serverUrl = serverUrl;
    }

    public String getServerUrls() {
        return serverUrls;
    }

    public void setServerUrls(String serverUrls) {
        this.serverUrls = serverUrls;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public Boolean getOldDataRequired() {
        return oldDataRequired;
    }

    public void setOldDataRequired(Boolean oldDataRequired) {
        this.oldDataRequired = oldDataRequired;
    }

    public boolean isConnectionUrlValid() {
        if (Strings.isNullOrEmpty(this.zkServers)
                && Strings.isNullOrEmpty(this.serverUrl)
                && Strings.isNullOrEmpty(this.serverUrls)) {
            return false;
        } else {
            return true;
        }
    }

    public static List<SocketAddress> convertUrlsToSocketAddressList(String serverUrls) throws ServerUrlsFormatException {
        List<SocketAddress> addresses = new ArrayList<>();

        if (StringUtils.isNotEmpty(serverUrls)) {
            for (String serverUrl : serverUrls.split(",")) {
                if (StringUtils.isNotEmpty(serverUrl)) {
                    try {
                        addresses.add(convertUrlToSocketAddress(serverUrl));
                    } catch (Exception exception) {
                        throw new ServerUrlsFormatException(String.format("The serverUrls are malformed . The ServerUrls : \"%s\" .", serverUrls), exception);
                    }
                }
            }
            return addresses;
        } else {
            return addresses;
        }
    }

    public static SocketAddress convertUrlToSocketAddress(String serverUrl) throws ServerUrlsFormatException, NumberFormatException {
        String[] hostAndPort = serverUrl.split(":");

        if (hostAndPort.length == 2 && StringUtils.isNotEmpty(hostAndPort[1])) {
            try {
                int port  = Integer.parseInt(hostAndPort[1]);
                InetSocketAddress socketAddress = new InetSocketAddress(hostAndPort[0], port);
                return socketAddress;

            } catch (NumberFormatException exception) {
                throw exception;
            }
        } else {
            throw new ServerUrlsFormatException(String.format("The serverUrl is malformed . The ServerUrl : \"%s\" .", serverUrl));
        }
    }
}