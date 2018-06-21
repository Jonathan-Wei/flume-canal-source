package com.google.common.collect;

import com.citic.helper.RegexHashMap;
import com.google.common.base.Supplier;
import java.io.Serializable;
import java.util.Map;

/**
 * The type Regex hash based table.
 *
 * @param <C> the type parameter
 * @param <V> the type parameter
 */
/*
 * 创建类似 Guava HashBasedTable 支持正则表达式的 Table
 * */
public class RegexHashBasedTable<C, V> extends StandardTable<String, C, V> {

    private static class Factory<C, V> implements Supplier<Map<C, V>>, Serializable {

        /**
         * The Expected size.
         */
        final int expectedSize;

        /**
         * Instantiates a new Factory.
         *
         * @param expectedSize the expected size
         */
        Factory(int expectedSize) {
            this.expectedSize = expectedSize;
        }

        @Override
        public Map<C, V> get() {
            return Maps.newHashMapWithExpectedSize(expectedSize);
        }

        private static final long serialVersionUID = 0;
    }

    /**
     * Creates an empty {@code HashBasedTable}.
     *
     * @param <C> the type parameter
     * @param <V> the type parameter
     * @return the regex hash based table
     */
    public static <C, V> RegexHashBasedTable<C, V> create() {
        return new RegexHashBasedTable<>(new RegexHashMap<>(), new Factory<C, V>(0));
    }


    /**
     * Create regex hash based table.
     *
     * @param <C> the type parameter
     * @param <V> the type parameter
     * @param table the table
     * @return the regex hash based table
     */
    public static <C, V> RegexHashBasedTable<C, V> create(
        Table<String, ? extends C, ? extends V> table) {
        RegexHashBasedTable<C, V> result = create();
        result.putAll(table);
        return result;
    }

    /**
     * Instantiates a new Regex hash based table.
     *
     * @param backingMap the backing map
     * @param factory the factory
     */
    RegexHashBasedTable(Map<String, Map<C, V>> backingMap, Factory<C, V> factory) {
        super(backingMap, factory);
    }

    private static final long serialVersionUID = 0;

    /**
     * The entry point of application.
     *
     * @param strings the input arguments
     */
    public static void main(String... strings) {
        RegexHashBasedTable<String, String> rh = RegexHashBasedTable.create();

        rh.put("test\\.test.*", "id", "11212");
        rh.put("test\\.test.*", "name", "zhoupeng");
    }
}
