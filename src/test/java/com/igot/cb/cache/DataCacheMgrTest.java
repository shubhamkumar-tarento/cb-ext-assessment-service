package com.igot.cb.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DataCacheMgrTest {

    private DataCacheMgr cacheMgr;

    @BeforeEach
    void setUp() {
        cacheMgr = new DataCacheMgr();
    }

    @Test
    void testPutAndGetStringInCache() {
        cacheMgr.putStringInCache("key1", "value1");
        assertEquals("value1", cacheMgr.getStringFromCache("key1"));
    }

    @Test
    void testGetStringFromCacheWhenKeyNotPresent() {
        assertEquals("", cacheMgr.getStringFromCache("missingKey"));
    }

    @Test
    void testPutAndGetObjectInCache() {
        Object obj = new Object();
        cacheMgr.putObjectInCache("objKey", obj);
        assertEquals(obj, cacheMgr.getObjectFromCache("objKey"));
    }

    @Test
    void testGetObjectFromCacheWhenKeyNotPresent() {
        assertNull(cacheMgr.getObjectFromCache("noObjKey"));
    }

    @Test
    void testPutAndGetContentInCache() {
        Map<String, Object> content = new HashMap<>();
        content.put("field", 123);
        cacheMgr.putContentInCache("contentKey", content);
        assertEquals(content, cacheMgr.getContentFromCache("contentKey"));
    }

    @Test
    void testGetContentFromCacheWhenKeyNotPresent() {
        Map<String, Object> content = cacheMgr.getContentFromCache("noContentKey");
        assertNotNull(content);
        assertTrue(content.isEmpty());
    }
}
