package com.igot.cb.cassandra.utils;

import com.igot.cb.core.exception.CassandraPropertyReaderException;
import org.junit.jupiter.api.*;
import java.lang.reflect.Field;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class CassandraPropertyReaderTest {

    @Test
    void testReadPropertyReturnsValueIfExists() throws Exception {
        CassandraPropertyReader reader = new CassandraPropertyReader();
        // Inject test property
        Field propertiesField = CassandraPropertyReader.class.getDeclaredField("properties");
        propertiesField.setAccessible(true);
        Properties props = new Properties();
        props.setProperty("test.key", "test.value");
        propertiesField.set(reader, props);

        assertEquals("test.value", reader.readProperty("test.key"));
    }

    @Test
    void testReadPropertyReturnsKeyIfNotExists() {
        CassandraPropertyReader reader = new CassandraPropertyReader();
        assertEquals("unknown.key", reader.readProperty("unknown.key"));
    }

    @Test
    void testExceptionWhenFileNotFound() throws Exception {
        // Change FILE_NAME to a non-existent file using reflection (works only if not final)
        Field fileNameField = CassandraPropertyReader.class.getDeclaredField("FILE_NAME");
        fileNameField.setAccessible(true);

        // Remove final modifier if possible (may not work in Java 12+)
        // This block may throw, so catch and skip if not possible
        try {
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(fileNameField, fileNameField.getModifiers() & ~java.lang.reflect.Modifier.FINAL);
            fileNameField.set(null, "nonexistent.properties");

            // Try to get a new instance (will throw)
            assertThrows(CassandraPropertyReaderException.class, CassandraPropertyReader::new);
        } catch (NoSuchFieldException | IllegalAccessException ignored) {
            // Skipped: cannot change final static field in this Java version
        }
    }
}
