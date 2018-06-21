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
    public static final String IP_INTERFACE = "ipInterface";
    public static final String ZOOKEEPER_SERVERS = "zkServers";
    public static final String SERVER_URL = "serverUrl";
    public static final String SERVER_URLS = "serverUrls";
    public static final String DESTINATION = "destination";
    public static final String USERNAME = "username";
    public static final String PSWD = "password";
    public static final String BATCH_SIZE = "batchSize";
    public static final String TABLE_TO_TOPIC_MAP = "tableToTopicMap";
    public static final String TABLE_FIELDS_FILTER = "tableFieldsFilter";

    public static final String TRANS_MAX_SPLIT_ROW_NUM = "transMaxSplitRowNum";
    public static final int DEFAULT_TRANS_MAX_SPLIT_ROW_NUM = 128;


    public static final String USE_AVRO = "useAvro";
    public static final String SHUTDOWN_FLOW_COUNTER = "shutdownFlowCounter";
    public static final boolean DEFAULT_SHUTDOWN_FLOW_COUNTER = false;
    public static final String DEFAULT_NOT_MAP_TOPIC = "cannot_map";
    public static final int DEFAULT_BATCH_SIZE = 1024;
    public static final int MIN_BATCH_SIZE = 128;
    public static final String DEFAULT_USERNAME = "";
    public static final String DEFAULT_PSWD = "";
    public static final String META_FIELD_TABLE = "__table";
    public static final String META_FIELD_TS = "__ts";
    public static final String META_FIELD_DB = "__db";
    public static final String META_FIELD_TYPE = "__type";
    public static final String META_FIELD_AGENT = "__agent";
    public static final String META_FIELD_FROM = "__from";
    public static final String META_FIELD_SQL = "__sql";
    public static final String META_TRANS_ID = "__transId";
    public static final String META_SPLIT_ID = "__splitId";
    public static final String META_DATA = "data";

    public static final String SUPPORT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
}
