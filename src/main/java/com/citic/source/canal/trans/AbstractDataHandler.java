package com.citic.source.canal.trans;

import static com.citic.source.canal.CanalSourceConstants.DECIMAL_FORMAT_3;
import static com.citic.source.canal.CanalSourceConstants.GSON;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_DB;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_TABLE;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_TS;
import static com.citic.source.canal.CanalSourceConstants.META_FIELD_TYPE;
import static com.citic.source.canal.CanalSourceConstants.TOKEN_TYPE;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.citic.source.canal.CanalConf;
import com.citic.source.canal.core.AbstractCommonDataHandler;
import com.citic.source.canal.core.TransDataHandlerInterface;
import com.google.common.collect.Maps;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractDataHandler extends AbstractCommonDataHandler implements
    TransDataHandlerInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataHandler.class);
    private final CanalConf canalConf;

    public AbstractDataHandler(CanalConf canalConf) {
        this.canalConf = canalConf;
    }

    /*
     * 处理行数据，并添加其他字段信息
     * */
    private Map<String, String> handleRowData(CanalEntry.RowData rowData,
        CanalEntry.Header entryHeader,
        CanalEntry.EventType eventType) {
        Map<String, String> eventMap = Maps.newHashMap();
        Map<String, String> rowDataMap;

        if (eventType == CanalEntry.EventType.DELETE) {
            // 删除事件 getAfterColumnsList 数据为空
            rowDataMap = convertColumnListToMap(rowData.getBeforeColumnsList(), entryHeader);
        } else {
            rowDataMap = convertColumnListToMap(rowData.getAfterColumnsList(), entryHeader);
        }

        eventMap.put(META_FIELD_TABLE, entryHeader.getTableName());
        eventMap.put(META_FIELD_TS, DECIMAL_FORMAT_3.format(System.currentTimeMillis() / 1000.0));
        eventMap.put(META_FIELD_DB, entryHeader.getSchemaName());
        eventMap.put(META_FIELD_TYPE, eventType.toString());

        eventMap.putAll(rowDataMap);
        return eventMap;
    }

    @Override
    public String getDataJsonString(CanalEntry.RowData rowData,
        CanalEntry.Header entryHeader,
        CanalEntry.EventType eventType) {

        // 处理行数据
        Map<String, String> eventData = handleRowData(rowData, entryHeader, eventType);
        LOGGER.debug("eventData handleRowData:{}", eventData);

        // TODO:对数据字段和表进行过滤
        return GSON.toJson(eventData, TOKEN_TYPE);
    }
}
