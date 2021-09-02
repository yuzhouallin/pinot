package org.apache.pinot.common.function.scalar;

import org.apache.pinot.common.function.scalar.utils.MapPathUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Map;


public class MapFunctions {
    private static Logger logger = LoggerFactory.getLogger(MapFunctions.class);

    @ScalarFunction
    public static Object mapPath(Map<String, Object> map, @NotNull String path, Object defaultValue) {
        try {
           ArrayList<String> keyList = MapPathUtils.mapKey(path);
            if (!keyList.isEmpty()) {
                for (String key : keyList) {
                    Object obj = map.get(key);
                    if (obj == null) {
                        break;
                    }
                    if (obj instanceof Map) {
                        map = (Map<String, Object>) obj;
                    } else {
                        return obj;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("transform error", e);
        }
        return defaultValue;
    }

    @ScalarFunction
    public static String mapPathString(Object object, @NotNull String path, String defaultValue) {
        if (object instanceof String) {
            return defaultValue;
        }
        if (object instanceof Map) {
            Object result = mapPath((Map<String, Object>) object, path, defaultValue);
            if (result instanceof Object[]) {
                return MapPathUtils.arrayToString((Object[]) result);
            }
            return String.valueOf(result);
        }
        return defaultValue;
    }

    @ScalarFunction
    public static long mapPathLong(Object object, @NotNull String path, long defaultValue) {
        if (object instanceof String) {
            return defaultValue;
        }
        if (object instanceof Map) {
            Object result = mapPath((Map<String, Object>) object, path, defaultValue);
            if (result instanceof Number) {
                return ((Number) result).longValue();
            }
            return Long.parseLong(String.valueOf(result));
        }
        return defaultValue;
    }


    @ScalarFunction
    public static double mapPathDouble(Object object, @NotNull String path, double defaultValue) {
        if (object instanceof String) {
            return defaultValue;
        }
        if (object instanceof Map) {
            Object result = mapPath((Map<String, Object>) object, path, defaultValue);
            return Double.parseDouble(String.valueOf(result));
        }
        return defaultValue;
    }
}
