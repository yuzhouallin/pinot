package org.apache.pinot.common.function;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.cache.Cache;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In the case of frequent data ingestion,
 * Not cache json path may be better than frequent swapping in and out of LRU.
 */
public class SimpleJsonPathMapCache implements Cache {

    private final ConcurrentMap<String, JsonPath> pathCache = new ConcurrentHashMap<String, JsonPath>(128, 0.75f, 1);

    private final int limit;

    public SimpleJsonPathMapCache(int limit) {
        this.limit = limit;
    }

    @Override
    public JsonPath get(String key) {
        return pathCache.get(key);
    }

    /**
     * cache JsonPath, if reached size limit, not cache it anymore.
     */
    @Override
    public void put(String key, JsonPath value) {
        if (pathCache.size() < limit) {
            // use putIfAbsent to handle concurrency put
            pathCache.putIfAbsent(key, value);
        }
    }
}
