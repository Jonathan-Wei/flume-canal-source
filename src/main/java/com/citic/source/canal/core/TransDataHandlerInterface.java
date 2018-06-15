package com.citic.source.canal.core;

import com.alibaba.otter.canal.protocol.CanalEntry;
import java.util.Map;

public interface TransDataHandlerInterface {

    Map<String, String> getDataMap(CanalEntry.RowData rowData,
        CanalEntry.Header entryHeader,
        CanalEntry.EventType eventType);

}
