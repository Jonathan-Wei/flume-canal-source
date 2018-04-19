package com.citic.helper;

import com.google.common.collect.Maps;
import org.apache.avro.Schema;

import java.util.Map;

/**
 * Created by zhoupeng on 2018/4/19.
 */
public class SchemaCache {
    private static final Map<String, Schema> schemaCache = Maps.newHashMap() ;
    private static final Schema.Parser parser = new Schema.Parser();

    public static Schema getSchema(String schemaString) {
        Schema schema;
        if (schemaCache.containsKey(schemaString)) {
            schema = schemaCache.get(schemaString);
        } else {
            schema  = parser.parse(schemaString);
            schemaCache.put(schemaString, schema);
        }
        return schema;
    }
}
