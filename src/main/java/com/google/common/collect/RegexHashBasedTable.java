package com.google.common.collect;

import com.citic.helper.RegexHashMap;
import com.google.common.base.Supplier;

import java.io.Serializable;
import java.util.Map;

/*
* 创建类似 Guava HashBasedTable 支持正则表达式的 Table
* */
public class RegexHashBasedTable<C,V> extends StandardTable<String, C, V> {

    private static class Factory<C, V> implements Supplier<Map<C, V>>, Serializable {
        final int expectedSize;

        Factory(int expectedSize) {
            this.expectedSize = expectedSize;
        }

        @Override
        public Map<C, V> get() {
            return Maps.newHashMapWithExpectedSize(expectedSize);
        }

        private static final long serialVersionUID = 0;
    }

    /** Creates an empty {@code HashBasedTable}. */
    public static <C, V> RegexHashBasedTable<C, V> create() {
        return new RegexHashBasedTable<>(new RegexHashMap<>(), new Factory<C, V>(0));
    }


    /**
     * Creates a {@code HashBasedTable} with the same mappings as the specified table.
     *
     * @param table the table to copy
     * @throws NullPointerException if any of the row keys, column keys, or values in {@code table} is
     *     null
     */
    public static <C, V> RegexHashBasedTable<C, V> create(
            Table<String, ? extends C, ? extends V> table) {
        RegexHashBasedTable<C, V> result = create();
        result.putAll(table);
        return result;
    }

    RegexHashBasedTable(Map<String, Map<C, V>> backingMap, Factory<C, V> factory) {
        super(backingMap, factory);
    }

    // Overriding so NullPointerTester test passes.

    @Override
    public boolean contains( Object rowKey,  Object columnKey) {
        return super.contains(rowKey, columnKey);
    }

    @Override
    public boolean containsColumn( Object columnKey) {
        return super.containsColumn(columnKey);
    }

    @Override
    public boolean containsRow( Object rowKey) {
        return super.containsRow(rowKey);
    }

    @Override
    public boolean containsValue( Object value) {
        return super.containsValue(value);
    }

    @Override
    public V get( Object rowKey,  Object columnKey) {
        return super.get(rowKey, columnKey);
    }

    @Override
    public boolean equals( Object obj) {
        return super.equals(obj);
    }

    @Override
    public V remove( Object rowKey,  Object columnKey) {
        return super.remove(rowKey, columnKey);
    }

    private static final long serialVersionUID = 0;

    public static void main(String...strings) {
        RegexHashBasedTable<String, Boolean> rh = RegexHashBasedTable.create();

        rh.put("test\\.test.*", "id", true);
        rh.put("test\\.test.*", "name", true);
        System.out.println(rh);

        System.out.println(rh.containsRow("test.test1"));
        System.out.println(rh.containsColumn("id"));
        System.out.println(rh.containsColumn("name"));
        System.out.println(rh.containsColumn("age"));
        System.out.println(rh);
    }
}
