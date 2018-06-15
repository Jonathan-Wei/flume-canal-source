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
import java.util.function.BiFunction;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Data handler.
 */
public class DataHandler extends AbstractCommonDataHandler implements
    TransDataHandlerInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataHandler.class);
    private final CanalConf canalConf;

    private final BiFunction<String, String, Boolean> removeColumnFilterFun;
    private final Function<String, Boolean> removeRowFilterFun;

    /**
     * Instantiates a new Data handler.
     *
     * @param canalConf the canal conf
     * @param removeRowFilterFun the remove row filter fun
     * @param removeColumnFilterFun the remove column filter fun
     */
    public DataHandler(CanalConf canalConf,
        Function<String, Boolean> removeRowFilterFun,
        BiFunction<String, String, Boolean> removeColumnFilterFun) {

        this.canalConf = canalConf;
        this.removeRowFilterFun = removeRowFilterFun;
        this.removeColumnFilterFun = removeColumnFilterFun;
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
            rowDataMap = convertColumnListToMap(rowData.getBeforeColumnsList(), entryHeader,
                this.removeColumnFilterFun);
        } else {
            rowDataMap = convertColumnListToMap(rowData.getAfterColumnsList(), entryHeader,
                this.removeColumnFilterFun);
        }

        eventMap.put(META_FIELD_TABLE, entryHeader.getTableName());
        eventMap.put(META_FIELD_TS, DECIMAL_FORMAT_3.format(System.currentTimeMillis() / 1000.0));
        eventMap.put(META_FIELD_DB, entryHeader.getSchemaName());
        eventMap.put(META_FIELD_TYPE, eventType.toString());

        eventMap.putAll(rowDataMap);
        return eventMap;
    }

    @Override
    public Map<String, String> getDataMap(CanalEntry.RowData rowData,
        CanalEntry.Header entryHeader,
        CanalEntry.EventType eventType) {

        String keyName = entryHeader.getSchemaName() + "." + entryHeader.getTableName();
        // 对行数据进行过滤
        if (this.removeRowFilterFun.apply(keyName)) {
            return null;
        }

        // 处理行数据
        Map<String, String> eventData = handleRowData(rowData, entryHeader, eventType);
        LOGGER.debug("eventData handleRowData:{}", eventData);

        return eventData;
    }
}
