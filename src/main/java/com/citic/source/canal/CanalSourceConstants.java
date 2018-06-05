package com.citic.source.canal;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.text.DecimalFormat;
import java.util.Map;

public class CanalSourceConstants {

    public static final Gson GSON = new Gson();
    public static final Type TOKEN_TYPE = new TypeToken<Map<String, Object>>() {}.getType();
    public static final DecimalFormat DECIMAL_FORMAT_3 = new DecimalFormat(".000");
    static final String IP_INTERFACE = "ipInterface";
    static final String ZOOKEEPER_SERVERS = "zkServers";
    static final String SERVER_URL = "serverUrl";
    static final String SERVER_URLS = "serverUrls";
    static final String DESTINATION = "destination";
    static final String USERNAME = "username";
    static final String PSWD = "password";
    static final String BATCH_SIZE = "batchSize";
    static final String TABLE_TO_TOPIC_MAP = "tableToTopicMap";
    static final String TABLE_FIELDS_FILTER = "tableFieldsFilter";
    static final String USE_AVRO = "useAvro";
    static final String SHUTDOWN_FLOW_COUNTER = "shutdownFlowCounter";
    static final boolean DEFAULT_SHUTDOWN_FLOW_COUNTER = false;
    static final String WRITE_SQL_TO_DATA = "writeSQLToData";
    static final boolean DEFAULT_WRITE_SQL_TO_DATA = false;
    static final String DEFAULT_NOT_MAP_TOPIC = "cannot_map";
    static final int DEFAULT_BATCH_SIZE = 1024;
    static final int MIN_BATCH_SIZE = 128;
    static final String DEFAULT_USERNAME = "";
    static final String DEFAULT_PSWD = "";
    static final String META_FIELD_TABLE = "__table";
    static final String META_FIELD_TS = "__ts";
    static final String META_FIELD_DB = "__db";
    static final String META_FIELD_TYPE = "__type";
    static final String META_FIELD_AGENT = "__agent";
    static final String META_FIELD_FROM = "__from";
    static final String META_FIELD_SQL = "__sql";

    static final String SUPPORT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
}
