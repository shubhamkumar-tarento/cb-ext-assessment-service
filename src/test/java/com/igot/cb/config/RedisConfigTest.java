package com.igot.cb.config;

import com.igot.cb.common.util.CbExtAssessmentServerProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RedisConfigTest {

    private CbExtAssessmentServerProperties properties;
    private RedisConfig redisConfig;

    @BeforeEach
    void setUp() {
        properties = mock(CbExtAssessmentServerProperties.class);
        when(properties.getRedisHostName()).thenReturn("localhost");
        when(properties.getRedisPort()).thenReturn("6379");
        when(properties.getRedisDataHostName()).thenReturn("localhost");
        when(properties.getRedisDataPort()).thenReturn("6380");
        when(properties.getMaxIdle()).thenReturn(10);
        when(properties.getMaxActive()).thenReturn(20);
        when(properties.getMinIdle()).thenReturn(5);
        when(properties.isTestOnBorrow()).thenReturn(true);
        when(properties.isTestOnReturn()).thenReturn(false);
        when(properties.isTestWhileIdle()).thenReturn(true);
        when(properties.getMinEvictableIdleTime()).thenReturn(60000L);
        when(properties.getTimeBetweenEvictionRuns()).thenReturn(30000L);
        when(properties.getNumTestsPerEvictionRun()).thenReturn(3);
        when(properties.isBlockWhenExhausted()).thenReturn(true);

        redisConfig = new RedisConfig(properties);
    }

    @Test
    void testJedisPoolBean() {
        JedisPool pool = redisConfig.jedisPool();
        assertNotNull(pool);
    }

    @Test
    void testJedisDataPopulationPoolBean() {
        JedisPool pool = redisConfig.jedisDataPopulationPool();
        assertNotNull(pool);
    }

    @Test
    void testBuildPoolConfig() throws Exception {
        java.lang.reflect.Method method = RedisConfig.class.getDeclaredMethod("buildPoolConfig");
        method.setAccessible(true);
        JedisPoolConfig config = (JedisPoolConfig) method.invoke(redisConfig);

        assertEquals(20, config.getMaxTotal());
        assertEquals(5, config.getMinIdle());
        assertTrue(config.getTestOnBorrow());
        assertFalse(config.getTestOnReturn());
        assertTrue(config.getTestWhileIdle());
        assertEquals(Duration.ofMillis(60000L), config.getMinEvictableIdleDuration());
        assertEquals(Duration.ofMillis(30000L), config.getTimeBetweenEvictionRuns());
        assertEquals(3, config.getNumTestsPerEvictionRun());
        assertTrue(config.getBlockWhenExhausted());
    }
}
