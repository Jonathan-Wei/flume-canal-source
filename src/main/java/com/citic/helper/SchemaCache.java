package com.citic.helper;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhoupeng on 2018/4/19.
 */
public class SchemaCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaCache.class);

    private static final Schema.Parser parser = new Schema.Parser();
    private static final LoadingCache<String, Schema> schemaCache = CacheBuilder
        .newBuilder()
        .maximumSize(10000)
        .build(
            new CacheLoader<String, Schema>() {
                @Override
                public Schema load(String schemaString) throws Exception {
                    return parser.parse(schemaString);
                }
            });

    /**
     * Gets schema.
     *
     * @param schemaString the schema string
     * @return the schema
     */
    public static Schema getSchema(String schemaString) {
        try {
            return schemaCache.get(schemaString);
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage(), e);
            return new Schema.Parser().parse(schemaString);
        }
    }
}
