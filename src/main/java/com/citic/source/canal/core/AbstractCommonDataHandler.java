package com.citic.source.canal.core;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public abstract class AbstractCommonDataHandler {

    /*
     * 对列数据进行解析
     * */
    protected Map<String, String> convertColumnListToMap(List<Column> columns,
        CanalEntry.Header entryHeader, BiFunction<String, String, Boolean> removeFilterFun) {
        Map<String, String> rowMap = Maps.newHashMap();

        String keyName = null;
        if (removeFilterFun != null) {
            keyName = entryHeader.getSchemaName() + "." + entryHeader.getTableName();
        }

        for (CanalEntry.Column column : columns) {
            String columnName = column.getName();

            if (removeFilterFun != null && removeFilterFun.apply(keyName, columnName)) {
                continue;
            }

            /*
             * date 2018-04-02
             * time 02:34:51
             * datetime 2018-04-02 11:43:16
             * timestamp 2018-04-02 11:45:02
             * mysql 默认格式如上，现在不做处理后续根据需要再更改
             * mysql datetime maps to a java.sql.Timestamp
             * */
            String colValue = column.getValue();
            rowMap.put(columnName, colValue);
        }
        return rowMap;
    }
}
