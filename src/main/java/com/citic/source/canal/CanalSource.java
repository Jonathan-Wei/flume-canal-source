package com.citic.source.canal;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.citic.source.canal.core.AbstractCanalSource;
import com.citic.source.canal.core.EntryConverterInterface;
import com.citic.source.canal.resolve.EntryConverter;
import java.util.List;
import org.apache.flume.Event;

public class CanalSource extends AbstractCanalSource {

    @Override
    protected EntryConverterInterface newEntryConverterInstance(boolean useAvro,
        CanalConf canalConf) {
        return new EntryConverter(useAvro, canalConf);
    }

    @Override
    protected void handleCanalEntry(Entry entry, CanalConf canalConf, List<Event> eventsAll,
        EntryConverterInterface entryConverter) {
        List<Event> events = entryConverter.convert(entry, canalConf);
        eventsAll.addAll(events);
    }
}
