package com.citic.source.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.flume.Event;

interface EntrySqlHandlerInterface {

    Event getSqlEvent(CanalEntry.Header entryHeader, String sql, CanalConf canalConf);
}