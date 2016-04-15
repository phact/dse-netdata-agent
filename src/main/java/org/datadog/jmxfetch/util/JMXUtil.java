package org.datadog.jmxfetch.util;

/**
 * Created by sebastianestevez on 4/14/16.
 */
public class JMXUtil {
    public static String getReadableClassName(String name) {
        String className = getArrayClassName(name);
        if (className == null) {
            return name;
        }
        int index = name.lastIndexOf("[");
        StringBuilder brackets = new StringBuilder(className);
        for (int i = 0; i <= index; i++) {
            brackets.append("[]");
        }
        return brackets.toString();
    }

    public static String getArrayClassName(String name) {
        String className = null;
        if (name.startsWith("[")) {
            int index = name.lastIndexOf("[");
            className = name.substring(index, name.length());
            if (className.startsWith("[L")) {
                className = className.substring(2, className.length() - 1);
            } else {
                try {
                    Class<?> c = Class.forName(className);
                    className = c.getComponentType().getName();
                } catch (ClassNotFoundException e) {
                    // Should not happen
                    throw new IllegalArgumentException(
                            "Bad class name " + name, e);
                }
            }
        }
        return className;
    }
}
