package com.igot.cb.config;

import com.igot.cb.common.util.CbExtAssessmentServerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

@Configuration
public class RedisConfig {
    final CbExtAssessmentServerProperties cbExtAssessmentServerProperties;

    public RedisConfig(CbExtAssessmentServerProperties cbExtAssessmentServerProperties) {
        this.cbExtAssessmentServerProperties = cbExtAssessmentServerProperties;
    }

    @Bean
    public JedisPool jedisPool() {
        return new JedisPool(buildPoolConfig(), cbExtAssessmentServerProperties.getRedisHostName(), Integer.parseInt(cbExtAssessmentServerProperties.getRedisPort()));
    }

    @Bean
    public JedisPool jedisDataPopulationPool() {
        final JedisPoolConfig poolConfig = buildPoolConfig();
        return new JedisPool(poolConfig, cbExtAssessmentServerProperties.getRedisDataHostName(),
                Integer.parseInt(cbExtAssessmentServerProperties.getRedisDataPort()));
    }
    private JedisPoolConfig buildPoolConfig() {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(cbExtAssessmentServerProperties.getMaxIdle());
        poolConfig.setMaxTotal(cbExtAssessmentServerProperties.getMaxActive());
        poolConfig.setMinIdle(cbExtAssessmentServerProperties.getMinIdle());
        poolConfig.setTestOnBorrow(cbExtAssessmentServerProperties.isTestOnBorrow());
        poolConfig.setTestOnReturn(cbExtAssessmentServerProperties.isTestOnReturn());
        poolConfig.setTestWhileIdle(cbExtAssessmentServerProperties.isTestWhileIdle());
        poolConfig.setMinEvictableIdleDuration(Duration.ofMillis(cbExtAssessmentServerProperties.getMinEvictableIdleTime()));
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(cbExtAssessmentServerProperties.getTimeBetweenEvictionRuns()));
        poolConfig.setNumTestsPerEvictionRun(cbExtAssessmentServerProperties.getNumTestsPerEvictionRun());
        poolConfig.setBlockWhenExhausted(cbExtAssessmentServerProperties.isBlockWhenExhausted());
        return poolConfig;
    }
}