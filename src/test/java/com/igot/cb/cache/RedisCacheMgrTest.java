package com.igot.cb.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.common.util.CbExtAssessmentServerProperties;
import com.igot.cb.common.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RedisCacheMgrTest {

    @InjectMocks
    private RedisCacheMgr redisCacheMgr;

    @Mock
    private JedisPool jedisPool;

    @Mock
    private JedisPool jedisDataPopulationPool;

    @Mock
    private CbExtAssessmentServerProperties cbExtAssessmentServerProperties;

    @Mock
    private Jedis jedis;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedisDataPopulationPool.getResource()).thenReturn(jedis);
        when(cbExtAssessmentServerProperties.getRedisQuestionsReadTimeOut()).thenReturn(84600);
        when(cbExtAssessmentServerProperties.getRedisTimeout()).thenReturn("84600");
        redisCacheMgr.postConstruct();
    }

    @Test
    void testPutAndGetCache() throws Exception {
        String key = "testKey";
        String value = "testValue";
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(value);

        when(jedis.set(anyString(), anyString())).thenReturn("OK");
        when(jedis.expire(anyString(), anyInt())).thenReturn(1L);
        when(jedis.get(Constants.REDIS_COMMON_KEY + key)).thenReturn(json);

        redisCacheMgr.putCache(key, value);
        String result = redisCacheMgr.getCache(key);
        assertEquals(json, result);
    }

    @Test
    void testPutStringInCacheAndGet() {
        String key = "strKey";
        String value = "strValue";
        when(jedis.set(anyString(), anyString())).thenReturn("OK");
        when(jedis.expire(anyString(), anyInt())).thenReturn(1L);
        when(jedis.get(Constants.REDIS_COMMON_KEY + key)).thenReturn(value);

        redisCacheMgr.putStringInCache(key, value);
        String result = redisCacheMgr.getCache(key);
        assertEquals(value, result);
    }

    @Test
    void testDeleteKeyByName() {
        String key = "delKey";
        when(jedis.del(Constants.REDIS_COMMON_KEY + key)).thenReturn(1L);

        boolean deleted = redisCacheMgr.deleteKeyByName(key);
        assertTrue(deleted);
    }

    @Test
    void testKeyExists() {
        String key = "existsKey";
        when(jedis.exists(Constants.REDIS_COMMON_KEY + key)).thenReturn(true);

        assertTrue(redisCacheMgr.keyExists(key));
    }

    @Test
    void testDeleteAllCBExtKey() {
        Set<String> keys = new HashSet<>(Arrays.asList(Constants.REDIS_COMMON_KEY + "a", Constants.REDIS_COMMON_KEY + "b"));
        when(jedis.keys(Constants.REDIS_COMMON_KEY + "*")).thenReturn(keys);
        when(jedis.del(anyString())).thenReturn(1L);

        assertTrue(redisCacheMgr.deleteAllCBExtKey());
    }

    @Test
    void testGetAllKeyNames() {
        Set<String> keys = new HashSet<>(Arrays.asList(Constants.REDIS_COMMON_KEY + "a", Constants.REDIS_COMMON_KEY + "b"));
        when(jedis.keys(Constants.REDIS_COMMON_KEY + "*")).thenReturn(keys);

        Set<String> result = redisCacheMgr.getAllKeyNames();
        assertEquals(keys, result);
    }

    @Test
    void testGetAllKeysAndValues() {
        Set<String> keys = Set.of(Constants.REDIS_COMMON_KEY + "a");
        when(jedis.keys(anyString())).thenReturn(keys);
        when(jedis.get(anyString())).thenReturn("val");
        List<Map<String, Object>> result = redisCacheMgr.getAllKeysAndValues();
        assertFalse(result.isEmpty());
        assertTrue(result.get(0).containsKey(Constants.REDIS_COMMON_KEY + "a"));
    }

    @Test
    void testGetHashedCacheFromDataRedis() {
        when(jedisDataPopulationPool.getResource()).thenReturn(jedis);
        when(jedis.hget(anyString(), anyString())).thenReturn("hashVal");
        String val = redisCacheMgr.getHashedCacheFromDataRedis("key", 1, "field");
        assertEquals("hashVal", val);
    }

    @Test
    void testGetCacheFromDataRedish() {
        when(jedisDataPopulationPool.getResource()).thenReturn(jedis);
        when(jedis.get(anyString())).thenReturn("dataVal");
        String val = redisCacheMgr.getCacheFromDataRedish("key", 1);
        assertEquals("dataVal", val);
    }

    @Test
    void testMget() {
        List<String> fields = List.of("1", "2");
        when(jedis.mget(any(String[].class))).thenReturn(List.of("a", "b"));
        List<String> result = redisCacheMgr.mget(fields);
        assertEquals(List.of("a", "b"), result);
    }

    @Test
    void testPutCacheAsStringArray() {
        String[] values = {"a", "b"};
        when(jedis.sadd(anyString(), any(String[].class))).thenReturn(1L);
        when(jedis.expire(anyString(), anyInt())).thenReturn(1L);
        redisCacheMgr.putCacheAsStringArray("key", values, 100);
        verify(jedis).sadd(Constants.REDIS_COMMON_KEY + "key", values);
        verify(jedis).expire(Constants.REDIS_COMMON_KEY + "key", 100L);
    }

    @Test
    void testPutInQuestionCache() {
        when(jedis.set(anyString(), anyString())).thenReturn("OK");
        when(jedis.expire(anyString(), anyInt())).thenReturn(1L);
        redisCacheMgr.putInQuestionCache("key", "val");
        verify(jedis).set(Constants.REDIS_COMMON_KEY + "key", "\"val\"");
    }

    @Test
    void testGetCacheWithIndex() {
        when(jedis.get(anyString())).thenReturn("val");
        String val = redisCacheMgr.getCache("key", 1);
        assertEquals("val", val);
    }

    @Test
    void testHget() {
        when(jedisDataPopulationPool.getResource()).thenReturn(jedis);
        when(jedis.hmget(anyString(), any(String[].class))).thenReturn(List.of("v1", "v2"));
        List<String> result = redisCacheMgr.hget("key", 1, "f1", "f2");
        assertEquals(List.of("v1", "v2"), result);
    }

    @Test
    void testGetSetFromCacheAsCommaSeparated() {
        when(jedis.smembers(anyString())).thenReturn(Set.of("a", "b"));
        Set<String> result = redisCacheMgr.getSetFromCacheAsCommaSeparated("key");
        assertEquals(Set.of("a", "b"), result);
    }

    @Test
    void testValueExists() {
        when(jedis.sismember(anyString(), anyString())).thenReturn(true);
        assertTrue(redisCacheMgr.valueExists("key", "val"));
    }

    @Test
    void testGetContentFromCache() {
        when(jedis.get(anyString())).thenReturn("content");
        String val = redisCacheMgr.getContentFromCache("key");
        assertEquals("content", val);
    }

    @Test
    void testPutCache_Success() {
        when(jedis.set(anyString(), anyString())).thenReturn("OK");
        when(jedis.expire(anyString(), anyInt())).thenReturn(1L);
        redisCacheMgr.putCache("key", Map.of("a", 1), 100);
        verify(jedis).set(contains(Constants.REDIS_COMMON_KEY), anyString());
        verify(jedis).expire(contains(Constants.REDIS_COMMON_KEY), eq(100L));
    }

    @Test
    void testPutCache_Exception() {
        doThrow(new RuntimeException("fail")).when(jedis).set(anyString(), anyString());
        redisCacheMgr.putCache("key", Map.of("a", 1), 100);
        // the failure is swallowed and logged, so the TTL is never applied
        verify(jedis, never()).expire(anyString(), anyLong());
    }

    @Test
    void testPutCache_DefaultTTL() {
        when(jedis.set(anyString(), anyString())).thenReturn("OK");
        when(jedis.expire(anyString(), anyInt())).thenReturn(1L);
        redisCacheMgr.putCache("key", Map.of("a", 1));
        verify(jedis).set(contains(Constants.REDIS_COMMON_KEY), anyString());
    }

    @Test
    void testPutInQuestionCache_Success() {
        when(jedis.set(anyString(), anyString())).thenReturn("OK");
        when(jedis.expire(anyString(), anyInt())).thenReturn(1L);
        redisCacheMgr.putInQuestionCache("key", Map.of("b", 2));
        verify(jedis).set(contains(Constants.REDIS_COMMON_KEY), anyString());
    }

    @Test
    void testPutStringInCache_WithTTL() {
        when(jedis.set(anyString(), anyString())).thenReturn("OK");
        when(jedis.expire(anyString(), anyInt())).thenReturn(1L);
        redisCacheMgr.putStringInCache("key", "value", 50);
        verify(jedis).set(contains(Constants.REDIS_COMMON_KEY), eq("value"));
        verify(jedis).expire(contains(Constants.REDIS_COMMON_KEY), eq(50L));
    }

    @Test
    void testPutStringInCache_DefaultTTL() {
        when(jedis.set(anyString(), anyString())).thenReturn("OK");
        when(jedis.expire(anyString(), anyInt())).thenReturn(1L);
        redisCacheMgr.putStringInCache("key", "value");
        verify(jedis).set(contains(Constants.REDIS_COMMON_KEY), eq("value"));
    }

    @Test
    void testDeleteKeyByName_Success() {
        when(jedis.del(anyString())).thenReturn(1L);
        boolean result = redisCacheMgr.deleteKeyByName("key");
        assertTrue(result);
        verify(jedis).del(contains(Constants.REDIS_COMMON_KEY));
    }

    @Test
    void testDeleteKeyByName_Exception() {
        doThrow(new RuntimeException("fail")).when(jedis).del(anyString());
        boolean result = redisCacheMgr.deleteKeyByName("key");
        assertFalse(result);
    }

    @Test
    void testDeleteAllCBExtKey_Success() {
        Set<String> keys = Set.of(Constants.REDIS_COMMON_KEY + "1", Constants.REDIS_COMMON_KEY + "2");
        when(jedis.keys(anyString())).thenReturn(keys);
        when(jedis.del(anyString())).thenReturn(1L);
        boolean result = redisCacheMgr.deleteAllCBExtKey();
        assertTrue(result);
        verify(jedis, times(keys.size())).del(anyString());
    }

    @Test
    void testDeleteAllCBExtKey_Exception() {
        when(jedis.keys(anyString())).thenThrow(new RuntimeException("fail"));
        boolean result = redisCacheMgr.deleteAllCBExtKey();
        assertFalse(result);
    }

    @Test
    void testGetCache_Success() {
        when(jedis.get(anyString())).thenReturn("value");
        String result = redisCacheMgr.getCache("key");
        assertEquals("value", result);
    }

    @Test
    void testGetCache_Exception() {
        when(jedis.get(anyString())).thenThrow(new RuntimeException("fail"));
        String result = redisCacheMgr.getCache("key");
        assertNull(result);
    }

    @Test
    void testMget_Success() {
        List<String> fields = List.of("1", "2");
        when(jedis.mget(any(String[].class))).thenReturn(List.of("v1", "v2"));
        List<String> result = redisCacheMgr.mget(fields);
        assertEquals(List.of("v1", "v2"), result);
    }

    @Test
    void testMget_Exception() {
        when(jedis.mget(any(String[].class))).thenThrow(new RuntimeException("fail"));
        List<String> result = redisCacheMgr.mget(List.of("1", "2"));
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetAllKeyNames_Success() {
        Set<String> keys = Set.of("k1", "k2");
        when(jedis.keys(anyString())).thenReturn(keys);
        Set<String> result = redisCacheMgr.getAllKeyNames();
        assertEquals(keys, result);
    }

    @Test
    void testGetAllKeyNames_Exception() {
        when(jedis.keys(anyString())).thenThrow(new RuntimeException("fail"));
        Set<String> result = redisCacheMgr.getAllKeyNames();
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetAllKeysAndValues_Success() {
        Set<String> keys = Set.of("k1", "k2");
        when(jedis.keys(anyString())).thenReturn(keys);
        when(jedis.get("k1")).thenReturn("v1");
        when(jedis.get("k2")).thenReturn("v2");
        List<Map<String, Object>> result = redisCacheMgr.getAllKeysAndValues();
        assertEquals(1, result.size());
        assertEquals("v1", result.get(0).get("k1"));
        assertEquals("v2", result.get(0).get("k2"));
    }

    @Test
    void testGetAllKeysAndValues_Exception() {
        when(jedis.keys(anyString())).thenThrow(new RuntimeException("fail"));
        List<Map<String, Object>> result = redisCacheMgr.getAllKeysAndValues();
        assertTrue(result.isEmpty());
    }

    @Test
    void testHget_Success() {
        when(jedis.hmget("key", "f1", "f2")).thenReturn(List.of("v1", "v2"));
        List<String> result = redisCacheMgr.hget("key", 1, "f1", "f2");
        assertEquals(List.of("v1", "v2"), result);
        verify(jedis).select(1);
    }

    @Test
    void testHget_Exception() {
        when(jedis.hmget(anyString(), any())).thenThrow(new RuntimeException("fail"));
        List<String> result = redisCacheMgr.hget("key", 1, "f1", "f2");
        assertTrue(result == null || result.isEmpty());
    }

    @Test
    void testGetCacheWithIndex_Success() {
        when(jedis.get("key")).thenReturn("value");
        String result = redisCacheMgr.getCache("key", 2);
        assertEquals("value", result);
        verify(jedis).select(2);
    }

    @Test
    void testGetCacheWithIndex_Exception() {
        when(jedis.get("key")).thenThrow(new RuntimeException("fail"));
        String result = redisCacheMgr.getCache("key", 2);
        assertNull(result);
    }

    @Test
    void testGetCacheFromDataRedish_Success() {
        when(jedis.get("key")).thenReturn("value");
        String result = redisCacheMgr.getCacheFromDataRedish("key", 3);
        assertEquals("value", result);
        verify(jedis).select(3);
    }

    @Test
    void testGetCacheFromDataRedish_Exception() {
        when(jedis.get("key")).thenThrow(new RuntimeException("fail"));
        String result = redisCacheMgr.getCacheFromDataRedish("key", 3);
        assertNull(result);
    }

    @Test
    void testGetHashedCacheFromDataRedis_Success() {
        when(jedis.hget("key", "field")).thenReturn("val");
        String result = redisCacheMgr.getHashedCacheFromDataRedis("key", 4, "field");
        assertEquals("val", result);
        verify(jedis).select(4);
    }

    @Test
    void testGetHashedCacheFromDataRedis_Exception() {
        when(jedis.hget(anyString(), anyString())).thenThrow(new RuntimeException("fail"));
        String result = redisCacheMgr.getHashedCacheFromDataRedis("key", 4, "field");
        assertNull(result);
    }

    @Test
    void testGetContentFromCache_Success() {
        when(jedis.get("key")).thenReturn("content");
        String result = redisCacheMgr.getContentFromCache("key");
        assertEquals("content", result);
    }

    @Test
    void testGetContentFromCache_Exception() {
        when(jedis.get("key")).thenThrow(new RuntimeException("fail"));
        String result = redisCacheMgr.getContentFromCache("key");
        assertNull(result);
    }

    @Test
    void testKeyExists_Success() {
        when(jedis.exists(anyString())).thenReturn(true);
        boolean result = redisCacheMgr.keyExists("key");
        assertTrue(result);
    }

    @Test
    void testKeyExists_Exception() {
        when(jedis.exists(anyString())).thenThrow(new RuntimeException("fail"));
        boolean result = redisCacheMgr.keyExists("key");
        assertFalse(result);
    }

    @Test
    void testValueExists_Success() {
        when(jedis.sismember(anyString(), anyString())).thenReturn(true);
        boolean result = redisCacheMgr.valueExists("key", "value");
        assertTrue(result);
    }

    @Test
    void testValueExists_Exception() {
        when(jedis.sismember(anyString(), anyString())).thenThrow(new RuntimeException("fail"));
        boolean result = redisCacheMgr.valueExists("key", "value");
        assertFalse(result);
    }

    @Test
    void testPutCacheAsStringArray_Exception() {
        doThrow(new RuntimeException("fail")).when(jedis).sadd(anyString(), any(String[].class));
        redisCacheMgr.putCacheAsStringArray("key", new String[]{"v1", "v2"}, 10);
        // the failure is swallowed and logged, so the TTL is never applied
        verify(jedis, never()).expire(anyString(), anyLong());
    }

    @Test
    void testGetSetFromCacheAsCommaSeparated_Success() {
        Set<String> set = Set.of("a", "b");
        when(jedis.smembers("key")).thenReturn(set);
        Set<String> result = redisCacheMgr.getSetFromCacheAsCommaSeparated("key");
        assertEquals(set, result);
        verify(jedis).select(10);
    }

    @Test
    void testGetSetFromCacheAsCommaSeparated_Exception() {
        when(jedis.smembers("key")).thenThrow(new RuntimeException("fail"));
        Set<String> result = redisCacheMgr.getSetFromCacheAsCommaSeparated("key");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
}
