package com.citic.source.canal.core;

import com.alibaba.otter.canal.protocol.CanalEntry;

public interface TransDataHandlerInterface {
    String getDataJsonString (CanalEntry.RowData rowData,
        CanalEntry.Header entryHeader,
        CanalEntry.EventType eventType);

}
