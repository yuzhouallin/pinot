package org.apache.pinot.common.function.scalar.utils;

import java.util.ArrayList;

public class MapPathUtils {
    public static ArrayList<String> mapKey(String path) {
        boolean flag = Boolean.FALSE;
        ArrayList<String> list = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < path.length(); i++) {
            if (flag == Boolean.FALSE) {
                if (path.charAt(i) == '.') {
                    if (path.charAt(i - 1) == ']') {
                        start++;
                        continue;
                    }
                    list.add(path.substring(start, i));
                    start = i + 1;
                } else if (path.charAt(i) == '[') {
                    if (i == 0) {
                        flag = Boolean.TRUE;
                        start = i + 1;
                        continue;
                    }
                    list.add(path.substring(start, i));
                    flag = Boolean.TRUE;
                    start = i + 1;
                } else if (i == path.length() - 1) {
                    list.add(path.substring(start, i + 1));
                    break;
                }
            } else {
                if (path.charAt(i) == ']') {
                    if (path.charAt(start) == '"' && path.charAt(i - 1) == '"') {
                        list.add(path.substring(start + 1, i - 1));
                    } else {
                        list.add(path.substring(start, i));
                    }
                    flag = Boolean.FALSE;
                    start = i + 1;
                }
            }
        }
        return list;
    }

    public static String arrayToString(Object[] a) {
        if (a == null) {
            return "null";
        }

        int iMax = a.length - 1;
        if (iMax == -1) {
            return "[]";
        }

        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = 0; ; i++) {
            b.append(String.valueOf(a[i]));
            if (i == iMax) {
                return b.append(']').toString();
            }
            b.append(",");
        }
    }

}
