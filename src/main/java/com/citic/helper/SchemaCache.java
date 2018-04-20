package com.citic.helper;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;

import java.util.Map;

/**
 * Created by zhoupeng on 2018/4/19.
 */
public class SchemaCache {
    private static final LoadingCache<String, Schema> schemaCache;
    private static final Schema.Parser parser = new Schema.Parser();

    static {
        schemaCache = CacheBuilder
                .newBuilder()
                .maximumSize(10000)
                .build(
            new CacheLoader<String, Schema>() {
                @Override
                public Schema load(String schemaString) {
                    return parser.parse(schemaString);
                }
            });
    }

    public static Schema getSchema(String schemaString) {
        return schemaCache.getUnchecked(schemaString);
    }
}
