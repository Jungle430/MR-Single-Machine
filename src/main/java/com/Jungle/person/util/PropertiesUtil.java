package com.Jungle.person.util;

import java.io.IOException;
import java.util.Properties;


public final class PropertiesUtil {
    private static final Properties prop;

    static {
        prop = new Properties();
        try {
            prop.load(PropertiesUtil.class.getClassLoader().getResourceAsStream("application.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getProperty(String key) {
        return prop.getProperty(key);
    }
}
