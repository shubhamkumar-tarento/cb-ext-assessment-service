package com.igot.cb.common.util;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Amit Kumar
 *
 * this class is used for reading properties file
 */
// Singleton is intentional here: this is a small, immutable, read-mostly cache of
// property files loaded once at class-init time and shared read-only by
// CassandraConnectionManagerImpl, AccessTokenValidator and KeyManager. A managed Spring
// bean is unnecessary since these callers are not Spring beans themselves, and the
// initialization-on-demand holder below is already thread-safe without extra locking.
@SuppressWarnings("java:S6548")
public class PropertiesCache {

    public final Map<String, Float> attributePercentageMap = new ConcurrentHashMap<>();
    private final String[] fileName = {
            "cassandra.config.properties",
            "cassandratablecolumn.properties",
            "application.properties"
    };
    private final Properties configProp = new Properties();

    /**
     * private default constructor
     */
    private PropertiesCache() {
        for (String file : fileName) {
            InputStream in = this.getClass().getClassLoader().getResourceAsStream(file);
            try {
                configProp.load(in);
            } catch (IOException e) {
                // Tolerate an unreadable file so the remaining property files still load.
            }
        }
    }

    /**
     * Initialization-on-demand holder. The JVM initialises a class lazily, once, and
     * under its own lock, so this is thread-safe with no synchronisation on the read
     * path and no volatile field to publish.
     */
    private static final class Holder {
        private static final PropertiesCache INSTANCE = new PropertiesCache();
    }

    public static PropertiesCache getInstance() {
        return Holder.INSTANCE;
    }

    public void saveConfigProperty(String key, String value) {
        configProp.setProperty(key, value);
    }

    public String getProperty(String key) {
        String value = System.getenv(key);
        if (StringUtils.isNotBlank(value)) return value;
        return configProp.getProperty(key) != null ? configProp.getProperty(key) : key;
    }

    /**
     * Method to read value from resource file .
     *
     * @param key
     * @return
     */
    public String readProperty(String key) {
        String value = System.getenv(key);
        if (StringUtils.isNotBlank(value)) return value;
        return configProp.getProperty(key);
    }

    public String readCustomError(String key) {
        if (StringUtils.isNoneBlank(key)) {
            key = key.replace(" ", "_");
        }
        return readProperty(key);
    }
}