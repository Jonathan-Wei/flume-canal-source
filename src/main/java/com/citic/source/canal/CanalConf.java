
package com.citic.source.canal;

import com.citic.helper.RegexHashMap;
import com.citic.helper.Utility;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CanalConf {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanalConf.class);
    private String agentIpAddress;
    private String zkServers;
    private String destination;
    private String username;
    private String password;
    private int batchSize;
    private String serverUrl;
    private String serverUrls;
    private String fromDbIp;

    private boolean shutdownFlowCounter;
    private boolean writeSqlToData;

    private String tableToTopicMap;
    private String tableFieldsFilter;

    // db.table -> topic
    private Map<String, String> tableToTopicRegexMap = new RegexHashMap<>();
    private List<String> filterTableList = Lists.newArrayList();

    static List<SocketAddress> convertUrlsToSocketAddressList(String serverUrls) throws
        IllegalArgumentException {
        List<SocketAddress> addresses = new ArrayList<>();
        if (StringUtils.isNotEmpty(serverUrls)) {
            for (String serverUrl : serverUrls.split(",")) {
                if (StringUtils.isNotEmpty(serverUrl)) {
                    try {
                        addresses.add(convertUrlToSocketAddress(serverUrl));
                    } catch (Exception exception) {
                        throw new IllegalArgumentException(
                            String.format("The serverUrls are malformed. "
                                + "The ServerUrls : \"%s\" .", serverUrls), exception);
                    }
                }
            }
            return addresses;
        } else {
            return addresses;
        }
    }

    static SocketAddress convertUrlToSocketAddress(String serverUrl)
        throws IllegalArgumentException,
        NumberFormatException {
        String[] hostAndPort = serverUrl.split(":");
        if (hostAndPort.length == 2 && StringUtils.isNotEmpty(hostAndPort[1])) {
            int port = Integer.parseInt(hostAndPort[1]);
            return new InetSocketAddress(hostAndPort[0], port);
        } else {
            throw new IllegalArgumentException(
                String.format("The serverUrl is malformed . The ServerUrl : \"%s\" .",
                    serverUrl));
        }
    }

    String getTableFieldsFilter() {
        return this.tableFieldsFilter;
    }

    void setTableFieldsFilter(String tableFieldsFilter) {
        this.tableFieldsFilter = tableFieldsFilter;
    }

    String getTableToTopicMap() {
        return this.tableToTopicMap;
    }

    void setTableToTopicMap(String tableToTopicMap) {
        this.tableToTopicMap = tableToTopicMap;
        splitTableToTopicMap(tableToTopicMap);
    }

    String getFromDbIp() {
        return this.fromDbIp;
    }

    private void splitTableToTopicMap(String tableToTopicMap) {
        Preconditions
            .checkArgument(!Strings.isNullOrEmpty(tableToTopicMap), "tableToTopicMap cannot empty");
        Splitter.on(';')
            .omitEmptyStrings()
            .trimResults()
            .split(tableToTopicMap)
            .forEach(item -> {
                String[] result = item.split(":");

                Preconditions.checkArgument(result.length >= 2,
                    "tableToTopicMap format incorrect json: db.tbl1:topic1;db.tbl2:topic2 "
                        + "avro: db.tbl1:topic1:schema1;db.tbl2:topic2:schema2");

                Preconditions.checkArgument(!Strings.isNullOrEmpty(result[0].trim()),
                    "db.table cannot empty");
                Preconditions.checkArgument(!Strings.isNullOrEmpty(result[1].trim()),
                    "topic cannot empty");

                filterTableList.add(result[0].trim());
                // db.table -> topic
                this.tableToTopicRegexMap.put(result[0].trim(), result[1].trim());
            });
    }

    /*
     * 根据表名获取 topic
     * */
    String getTableTopic(String schemaTableName) {
        if (this.tableToTopicRegexMap != null) {
            return this.tableToTopicRegexMap
                .getOrDefault(schemaTableName, CanalSourceConstants.DEFAULT_NOT_MAP_TOPIC);
        } else {
            return CanalSourceConstants.DEFAULT_NOT_MAP_TOPIC;
        }
    }

    boolean isWriteSqlToData() {
        return writeSqlToData;
    }

    void setWriteSqlToData(boolean writeSqlToData) {
        this.writeSqlToData = writeSqlToData;
    }

    boolean isShutdownFlowCounter() {
        return shutdownFlowCounter;
    }

    void setShutdownFlowCounter(boolean shutdownFlowCounter) {
        this.shutdownFlowCounter = shutdownFlowCounter;
    }

    String getAgentIpAddress() {
        return agentIpAddress;
    }

    String getZkServers() {
        return zkServers;
    }

    void setZkServers(String zkServers) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(zkServers), "zkServers cannot empty");
        this.zkServers = zkServers;
    }

    String getDestination() {
        return destination;
    }

    void setDestination(String destination) {
        Preconditions
            .checkArgument(!Strings.isNullOrEmpty(destination), "destination cannot empty");
        this.destination = destination;
        // destination example: 192_168_2_24-3306
        this.fromDbIp = this.destination.replace("-", ":").replace("_", ".");
    }

    String getUsername() {
        return username;
    }

    void setUsername(String username) {
        this.username = username;
    }

    String getPassword() {
        return password;
    }

    void setPassword(String password) {
        this.password = password;
    }

    int getBatchSize() {
        return batchSize;
    }

    void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    String getServerUrl() {
        return serverUrl;
    }

    void setServerUrl(String serverUrl) {
        this.serverUrl = serverUrl;
    }

    String getServerUrls() {
        return serverUrls;
    }

    void setServerUrls(String serverUrls) {
        this.serverUrls = serverUrls;
    }

    void setIpInterface(String ipInterface) {
        agentIpAddress = Utility.getLocalIp(ipInterface);
    }

    /*
     * 获取需要过滤的表列表
     * */
    List<String> getFilterTableList() {
        return filterTableList;
    }

    boolean isConnectionUrlValid() {
        return !(Strings.isNullOrEmpty(this.zkServers)
            && Strings.isNullOrEmpty(this.serverUrl)
            && Strings.isNullOrEmpty(this.serverUrls));
    }
}
