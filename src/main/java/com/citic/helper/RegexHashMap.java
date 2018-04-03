package com.citic.helper;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.regex.Pattern;

public class RegexHashMap<V> implements Map<String, V>
{
    private class PatternMatcher
    {
        private final String regex;
        private final Pattern compiled;
        PatternMatcher(String name) {
            regex = name;
            compiled = Pattern.compile(regex);
        }

        boolean matched(String string) {
            if(compiled.matcher(string).matches()) {
                cacheRef.put(string, regex);
                return true;
            }
            return false;
        }
    }
    /**
     * Map of input to pattern
     */
    private final Map<String, String> cacheRef = new WeakHashMap<>();
    /**
     * Map of pattern to value
     */
    private final Map<String, V> contentMap = new HashMap<>();
    /**
     * Compiled patterns
     */
    private final List<PatternMatcher> matchers = new ArrayList<PatternMatcher>();

    @Override
    public String toString() {
        return "RegexHashMap [cacheRef=" + cacheRef + ", contentMap=" + contentMap + "]";
    }
    /**
     *
     */
    public RegexHashMap() {}
    /**
     * Returns the value to which the specified key pattern is mapped, or null if this contentMap contains no mapping
     for the key pattern
     */
    @Override
    public V get(Object weakKey) {
        if(!cacheRef.containsKey(weakKey)) {
            for(PatternMatcher matcher : matchers) {
                if(matcher.matched((String) weakKey)) {
                    break;
                }
            }
        }
        if(cacheRef.containsKey(weakKey)) {
            return contentMap.get(cacheRef.get(weakKey));
        }
        // 再次将正则表达式传递进来也可以获得值
        if(contentMap.containsKey(weakKey)) {
            return contentMap.get(weakKey);
        }
        return null;
    }
    /**
     * Associates a specified regular expression to a particular value
     */
    @Override
    public V put(String key, V value) {
        V v = contentMap.put(key, value);
        if (v == null) {
            matchers.add(new PatternMatcher(key));
        }
        return v;
    }
    /**
     * Removes the regular expression key
     */
    @Override
    public V remove(Object key) {
        V v = contentMap.remove(key);
        if(v != null) {
            for(Iterator<PatternMatcher> iter = matchers.iterator(); iter.hasNext();) {
                PatternMatcher matcher = iter.next();
                if(matcher.regex.equals(key)) {
                    iter.remove();
                    break;
                }
            }
            for(Iterator<Entry<String, String>> iter = cacheRef.entrySet().iterator(); iter.hasNext();) {
                Entry<String, String> entry = iter.next();
                if(entry.getValue().equals(key)) {
                    iter.remove();
                }
            }
        }
        return v;
    }

    @Override
    public void putAll(Map<? extends String, ? extends V> m) {
        for (Map.Entry<? extends String, ? extends V> e : m.entrySet()) {
            String key = e.getKey();
            V value = e.getValue();
            put(key, value);
        }
    }

    /**
     * Set of view on the regular expression keys
     */
    @Override
    public Set<Entry<String,V>> entrySet() {
        return contentMap.entrySet();
    }

    @Override
    public int size() {
        return contentMap.size();
    }

    @Override
    public boolean isEmpty() {
        return contentMap.isEmpty();
    }

    /**
     * Returns true if this contentMap contains a mapping for the specified regular expression key.
     */
    @Override
    public boolean containsKey(Object key) {
        if(!cacheRef.containsKey(key)) {
            for(PatternMatcher matcher : matchers) {
                if(matcher.matched((String) key)) {
                    break;
                }
            }
        }
        return cacheRef.containsKey(key) || contentMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return contentMap.containsValue(value);
    }

    @Override
    public void clear() {
        contentMap.clear();
        matchers.clear();
        cacheRef.clear();
    }

    /**
     * Returns a Set view of the regular expression keys contained in this contentMap.
     */
    @Override
    public Set keySet() {
        return contentMap.keySet();
    }

    /**
     * Returns a Set view of the regex matched patterns contained in this contentMap. The set is backed by the contentMap, so changes to
     the contentMap are reflected in the set, and vice-versa.
     * @return
     */
    public Set<String> keySetPattern() {
        return cacheRef.keySet();
    }

    @Override
    public Collection values() {
        return contentMap.values();
    }

    /**
     * Produces a contentMap of patterns to values, based on the regex put in this contentMap
     * @param patterns
     * @return
     */
    public Map<String, V> transform(List<String> patterns) {
        for(String pattern : patterns) {
            get(pattern);
        }
        Map<String, V> transformed = new HashMap<>();
        for(Entry<String, String> entry : cacheRef.entrySet()) {
            transformed.put(entry.getKey(), contentMap.get(entry.getValue()));
        }
        return transformed;
    }

    public static void main(String...strings) {
        Map<String, String> temp = Maps.newHashMap();
        temp.put("[o|O][s|S][e|E].?[1|2]", "This is a regex match");
        temp.put("account", "This is a direct match");
        temp.put("test\\.test.*", "test db test starts tables");

        RegexHashMap<String> rh = new RegexHashMap<String>();
        rh.putAll(temp);
        System.out.println(rh);
        System.out.println("get:ose-1 -> "+rh.get("ose-1"));
        System.out.println("get:OSE2 -> "+rh.get("OSE2"));
        System.out.println("get:OSE112 -> "+rh.get("OSE112"));
        System.out.println("get:ose-2 -> "+rh.get("ose-2"));
        System.out.println("get:account -> "+rh.get("account"));
        System.out.println("get:test.test2 -> "+rh.get("test.test2"));
        System.out.println("get:test\\.test.* -> "+rh.get("test\\.test.*"));
        System.out.println(rh);

    }
}
