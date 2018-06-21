package com.citic.helper;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhoupeng on 2018/4/19.
 */
public class SchemaCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaCache.class);
    private static final Map<String, Schema> schemaCache = Maps.newConcurrentMap();

    private static String getTableFieldSchema(Iterable<String> schemaFieldList, String schemaName) {
        StringBuilder builder = new StringBuilder();
        String schema = "{"
            + "\"type\":\"record\","
            + "\"name\":\"" + schemaName + "\","
            + "\"fields\":[";

        builder.append(schema);

        String prefix = "";
        for (String fieldStr : schemaFieldList) {
            String field = "{ \"name\":\"" + fieldStr + "\", \"type\":[\"string\",\"null\"] }";
            builder.append(prefix);
            prefix = ",";
            builder.append(field);
        }

        builder.append("]}");
        return builder.toString();
    }

    /**
     * Gets schema.
     *
     * @param schemaFieldList the schema field list
     * @param schemaName the schema name
     * @return the schema
     */
    public static Schema getSchema(Iterable<String> schemaFieldList, String schemaName) {
        return schemaCache.computeIfAbsent(schemaName, key -> {
            String schemaString = getTableFieldSchema(schemaFieldList, schemaName);
            Schema.Parser parser = new Schema.Parser();
            return parser.parse(schemaString);
        });
    }

    /**
     * Gets schema 2.
     *
     * @param schemaFieldList the schema field list
     * @param attrList the attr list
     * @param schemaName the schema name
     * @return the schema 2
     */
    public static Schema getSchema2(Iterable<String> schemaFieldList, Iterable<String> attrList,
        String schemaName) {
        return schemaCache.computeIfAbsent(schemaName, key -> {
            String schemaString = getTableFieldSchema(Iterables.unmodifiableIterable(
                Iterables.concat(schemaFieldList, attrList)), schemaName);
            Schema.Parser parser = new Schema.Parser();
            return parser.parse(schemaString);
        });
    }

    /**
     * Gets schema sink.
     *
     * @param schemaName the schema name
     * @return the schema sink
     */
    public static Schema getSchemaSink(String schemaName) {
        return schemaCache.get(schemaName);
    }


    /**
     * Clear schema cache.
     */
    public static void clearSchemaCache() {
        schemaCache.clear();
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {
        List<String> tst = Lists.newArrayList("id", "name", "age");

        Schema test = getSchema2(tst, tst, "zhoupeng");
        System.out.println(test);
    }
}
