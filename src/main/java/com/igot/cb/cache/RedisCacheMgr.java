package com.igot.cb.cache;



import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.common.util.CbExtAssessmentServerProperties;
import com.igot.cb.common.util.Constants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.annotation.PostConstruct;
import java.util.*;

@Component
public class RedisCacheMgr {

    private static final String CACHE_KEY_SAVED_LOG = "Cache_key_value {}{} is saved in redis";

    private int cacheTtl = 84600;

    private final JedisPool jedisPool;

    private final JedisPool jedisDataPopulationPool;

    final CbExtAssessmentServerProperties cbExtAssessmentServerProperties;

    private final Logger logger = LoggerFactory.getLogger(RedisCacheMgr.class);

    ObjectMapper objectMapper = new ObjectMapper();

    private int questionsCacheTtl = 84600;

    public RedisCacheMgr(JedisPool jedisPool, JedisPool jedisDataPopulationPool,
            CbExtAssessmentServerProperties cbExtAssessmentServerProperties) {
        this.jedisPool = jedisPool;
        this.jedisDataPopulationPool = jedisDataPopulationPool;
        this.cbExtAssessmentServerProperties = cbExtAssessmentServerProperties;
    }

    @PostConstruct
    public void postConstruct() {
        this.questionsCacheTtl = cbExtAssessmentServerProperties.getRedisQuestionsReadTimeOut().intValue();
        if (!StringUtils.isEmpty(cbExtAssessmentServerProperties.getRedisTimeout())) {
            cacheTtl = Integer.parseInt(cbExtAssessmentServerProperties.getRedisTimeout());
        }
    }
    public void putCache(String key, Object object, int ttl) {
        try (Jedis jedis = jedisPool.getResource()) {
            String data = objectMapper.writeValueAsString(object);
            jedis.set(Constants.REDIS_COMMON_KEY + key, data);
            jedis.expire(Constants.REDIS_COMMON_KEY + key, ttl);
            logger.debug(CACHE_KEY_SAVED_LOG, Constants.REDIS_COMMON_KEY, key);
        } catch (Exception e) {
            logger.error("Error while putting cache data in Redis cache: ", e);
        }
    }
    public void putCache(String key, Object object) {
        putCache(key,object,cacheTtl);
    }
    public void putInQuestionCache(String key, Object object) {
        try (Jedis jedis = jedisPool.getResource()) {
            String data = objectMapper.writeValueAsString(object);
            jedis.set(Constants.REDIS_COMMON_KEY + key, data);
            jedis.expire(Constants.REDIS_COMMON_KEY + key, questionsCacheTtl);
            logger.debug(CACHE_KEY_SAVED_LOG, Constants.REDIS_COMMON_KEY, key);
        } catch (Exception e) {
            logger.error("Error while putting Question data in Redis cache: ", e);
        }
    }
    public void putStringInCache(String key, String value,int ttl) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(Constants.REDIS_COMMON_KEY + key, value);
            jedis.expire(Constants.REDIS_COMMON_KEY + key, ttl);
            logger.debug(CACHE_KEY_SAVED_LOG, Constants.REDIS_COMMON_KEY, key);
        } catch (Exception e) {
            logger.error("Error while putting data in Redis cache: ", e);
        }
    }

    public void putStringInCache(String key, String value) {
        putStringInCache(key, value, cacheTtl);
    }

    public boolean deleteKeyByName(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
        	jedis.del(Constants.REDIS_COMMON_KEY + key);
            logger.debug("Cache_key_value {}{} is deleted from redis", Constants.REDIS_COMMON_KEY, key);
            return true;
        } catch (Exception e) {
            logger.error("Error while delete by key Name data in Redis cache: ", e);
            return false;
        }
    }

    public boolean deleteAllCBExtKey() {
        try (Jedis jedis = jedisPool.getResource()) {
            String keyPattern = Constants.REDIS_COMMON_KEY + "*";
            Set<String> keys = jedis.keys(keyPattern);
            for (String key : keys) {
            	jedis.del(key);
            }
            logger.info("All Keys starts with " + Constants.REDIS_COMMON_KEY + " is deleted from redis");
            return true;
        } catch (Exception e) {
            logger.error("Error while delete all data in Redis cache: ", e);
            return false;
        }
    }

    public String getCache(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(Constants.REDIS_COMMON_KEY + key);
        } catch (Exception e) {
            logger.error("Error while getting data from Redis cache: ", e);
            return null;
        }
    }

    // null is returned (instead of an empty list) on failure so callers can tell a Redis outage
    // apart from a genuine cache miss and fall back to fetching from the source of truth.
    @SuppressWarnings("java:S1168")
    public List<String> mget(List<String> fields) {
        try (Jedis jedis = jedisPool.getResource()) {
        	String[] updatedKeys = new String[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
            	updatedKeys[i] = Constants.REDIS_COMMON_KEY + Constants.QUESTION_ID + fields.get(i);
            }
            return jedis.mget(updatedKeys);
        } catch (Exception e) {
            logger.error("Error while getting all data from Redis cache: ", e);
        }
        return null;
    }

    public Set<String> getAllKeyNames() {
        try (Jedis jedis = jedisPool.getResource()) {
            String keyPattern = Constants.REDIS_COMMON_KEY + "*";
            return jedis.keys(keyPattern);
        } catch (Exception e) {
            logger.error("Error while getting all key Names from Redis cache: ", e);
            return Collections.emptySet();
        }
    }

    public List<Map<String, Object>> getAllKeysAndValues() {
        List<Map<String, Object>> result = new ArrayList<>();
        try (Jedis jedis = jedisPool.getResource()) {
            String keyPattern = Constants.REDIS_COMMON_KEY + "*";
            Map<String, Object> res = new HashMap<>();
            Set<String> keys = jedis.keys(keyPattern);
            if (!keys.isEmpty()) {
                for (String key : keys) {
                    Object entries;
                    entries = jedis.get(key);
                    res.put(key, entries);
                }
                result.add(res);
            }
        } catch (Exception e) {
            logger.error("Error while getting all key and values from Redis cache: ", e);
            return Collections.emptyList();
        }
        return result;
    }
    
    public List<String> hget(String key, int index, String... fields) {
        try (Jedis jedis = jedisDataPopulationPool.getResource()) {
            jedis.select(index);
            return jedis.hmget(key, fields);
        } catch (Exception e) {
            logger.error("Error while getting index list from Redis cache: ", e);
            return Collections.emptyList();
        }
    }

    public String getCache(String key, Integer index) {
        try (Jedis jedis = jedisPool.getResource()) {
            if (index != null) {
                jedis.select(index);
            }
            return jedis.get(key);
        } catch (Exception e) {
            logger.error("Error while getting Index from Redis cache: ", e);
            return null;
        }
    }

    public String getCacheFromDataRedish(String key, Integer index) {
        try (Jedis jedis = jedisDataPopulationPool.getResource()) {
            if (index != null) {
                jedis.select(index);
            }
            return jedis.get(key);
        } catch (Exception e) {
            logger.error("Failed to get key '{}' from Redis at index {}: {}", key, index, e.getMessage(), e);
            return null;
        }
    }

    public String getHashedCacheFromDataRedis(String key, Integer index, String field) {
        try (Jedis jedis = jedisDataPopulationPool.getResource()) {
            if (index != null) {
                jedis.select(index);
            }
            return jedis.hget(key,field);
        } catch (Exception e) {
            logger.error("Failed to fetch field '{}' from Redis hash '{}' at index {}: {}", field, key, index, e.getMessage(), e);
            return null;
        }
    }

    public String getContentFromCache(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        } catch (Exception e) {
            logger.error("Failed to fetch Content from Redis cache: ", e);
            return null;
        }
    }

    public boolean keyExists(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(Constants.REDIS_COMMON_KEY + key);
        } catch (Exception e) {
            logger.error("An Error Occurred while fetching value from Redis", e);
            return false;
        }
    }

    public boolean valueExists(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.sismember(Constants.REDIS_COMMON_KEY + key, value);
        } catch (Exception e) {
            logger.error("An Error Occurred while fetching value from Redis", e);
            return false;
        }
    }

    public void putCacheAsStringArray(String key, String[] values, Integer ttl) {
        try (Jedis jedis = jedisPool.getResource()) {
            if(null == ttl)
                ttl = cacheTtl;
            jedis.sadd(Constants.REDIS_COMMON_KEY + key, values);
            jedis.expire(Constants.REDIS_COMMON_KEY + key, ttl);
            logger.debug(CACHE_KEY_SAVED_LOG, Constants.REDIS_COMMON_KEY, key);
        } catch (Exception e) {
            logger.error("An error occurred while saving data into Redis",e);
        }
    }

    public Set<String> getSetFromCacheAsCommaSeparated(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.select(10);
            return jedis.smembers(key);
        } catch (Exception e) {
            logger.error("Failed to fetch Set from Redis cache: ", e);
            return Collections.emptySet();
        }
    }
}
