/*
 * Copyright 2011-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hibernate.cache.redis.jedis;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.cache.redis.serializer.RedisSerializer;
import org.hibernate.cache.redis.serializer.SerializationTool;
import org.hibernate.cache.redis.serializer.SnappyRedisSerializer;
import org.hibernate.cache.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * RedisClient implements using Jedis library
 * <p>
 * 참고 : https://github.com/xetorthio/org.hibernate.cache.redis.jedis/wiki/AdvancedUsage
 *
 * @author 배성혁 ( sunghyouk.bae@gmail.com )
 * @since 13. 4. 9 오후 10:20
 */
@Slf4j
public class JedisClient {

  private static final int PAGE_SIZE = 128;
  public static final int DEFAULT_EXPIRY_IN_SECONDS = 120;
  public static final String DEFAULT_REGION_NAME = "hibernate";

  @Getter
  private final JedisPool jedisPool;

  @Getter
  @Setter
  private int expiryInSeconds;

  private final StringRedisSerializer regionSerializer = new StringRedisSerializer();
  private final StringRedisSerializer keySerializer = new StringRedisSerializer();
  private final RedisSerializer<Object> valueSerializer = new SnappyRedisSerializer<Object>();

  public JedisClient() {
    this(new JedisPool("localhost"), DEFAULT_EXPIRY_IN_SECONDS);
  }

  public JedisClient(JedisPool jedisPool) {
    this(jedisPool, DEFAULT_EXPIRY_IN_SECONDS);
  }

  /**
   * initialize JedisClient instance
   *
   * @param jedisPool       JedisPool instance
   * @param expiryInSeconds expiration in seconds
   */
  public JedisClient(JedisPool jedisPool, int expiryInSeconds) {
    log.debug("JedisClient created. jedisPool=[{}], expiryInSeconds=[{}]", jedisPool, expiryInSeconds);

    this.jedisPool = jedisPool;
    this.expiryInSeconds = expiryInSeconds;
  }

  /**
   * ping test for server alive
   */
  public String ping() {
    return run(new JedisCallback<String>() {
      @Override
      public String execute(Jedis jedis) {
        return jedis.ping();
      }
    });
  }

  /**
   * get Redis db size
   */
  public Long dbSize() {
    return run(new JedisCallback<Long>() {
      @Override
      public Long execute(Jedis jedis) {
        return jedis.dbSize();
      }
    });
  }

  /**
   * confirm the specified cache item in specfied region
   *
   * @param region region name
   * @param key    cache key
   */
  public boolean exists(final String region, final Object key) {
    final byte[] rawRegion = rawRegion(region);
    final byte[] rawKey = rawKey(key);
    final byte[] regionedKey = getRegionedKey(rawRegion, rawKey);

    return run(new JedisCallback<Boolean>() {
      @Override
      public Boolean execute(Jedis jedis) {
        return jedis.exists(regionedKey);
      }
    });
  }

  /**
   * Get cache
   *
   * @param region region name
   * @param key    cache key
   * @return return cached entity, if not exists return null.
   */
  public Object get(final String region, final Object key) {
    return get(region, key, 0);
  }

  /**
   * Get cache
   *
   * @param region              region name
   * @param key                 cache key
   * @param expirationInSeconds expiration timeout in seconds
   * @return return cached entity, if not exists return null.
   */
  public Object get(final String region, final Object key, final int expirationInSeconds) {
    final byte[] rawRegion = rawRegion(region);
    final byte[] rawKey = rawKey(key);
    final byte[] regionedKey = getRegionedKey(rawRegion, rawKey);

    byte[] rawValue = run(new JedisCallback<byte[]>() {
      @Override
      public byte[] execute(Jedis jedis) {
        return jedis.get(regionedKey);
      }
    });

    // after get, update expiration time
    if (rawValue != null && rawValue.length > 0) {
      if (expirationInSeconds > 0 && !region.contains("UpdateTimestampsCache")) {
        run(new JedisCallback<Object>() {
          @Override
          public Object execute(Jedis jedis) {
            final byte[] rawKnownKeysKey = rawKnownKeyskey(region);
            final long score = System.currentTimeMillis() + expirationInSeconds * 1000L;
            return jedis.zadd(rawKnownKeysKey, score, rawKey);
          }
        });
      }
    }

    return deserializeValue(rawValue);
  }

  private Boolean isExpired(final String region, final Object key) {
    final byte[] rawKnownKeysKey = rawKnownKeyskey(region);
    final byte[] rawKey = rawKey(key);

    Double timestamp = run(new JedisCallback<Double>() {
      @Override
      public Double execute(Jedis jedis) {
        return jedis.zscore(rawKnownKeysKey, rawKey);
      }
    });
    return timestamp != null && System.currentTimeMillis() > timestamp.longValue();
  }

  /**
   * retrieve all cached items in specified region
   *
   * @param region region
   * @return collection of cached items
   */
  public Set<Object> keysInRegion(String region) {
    try {
      final byte[] rawKnownKeysKey = rawKnownKeyskey(region);
      Set<byte[]> rawKeys = run(new JedisCallback<Set<byte[]>>() {
        @Override
        public Set<byte[]> execute(Jedis jedis) {
          int offset = 0;
          boolean finished = false;
          Set<byte[]> rawKeys = new HashSet<byte[]>();

          do {
            // need to paginate the keys
            Set<byte[]> tmpRawKeys = jedis.zrange(rawKnownKeysKey, (offset) * PAGE_SIZE, (offset + 1) * PAGE_SIZE - 1);
            rawKeys.addAll(tmpRawKeys);
            finished = tmpRawKeys.size() < PAGE_SIZE;
            offset++;
          } while (!finished);

          return rawKeys;
        }
      });

      return deserializeKeys(rawKeys);
    } catch (Exception ignored) {
    }
    return new HashSet<Object>();
  }

  /**
   * get cache count in region
   *
   * @param region region
   * @return cache item count in region
   */

  public Long keySizeInRegion(final String region) {
    final byte[] rawKnownKeysKey = rawKnownKeyskey(region);
    return run(new JedisCallback<Long>() {
      @Override
      public Long execute(Jedis jedis) {
        return jedis.zcard(rawKnownKeysKey);
      }
    });
  }

  /**
   * get all cached items in specified region
   *
   * @param region region name
   * @return map of keys and all cached items in specified region
   */
  public Map<Object, Object> hgetAll(String region) {
    final byte[] rawRegion = rawRegion(region);
    final byte[] rawKnownKeysKey = rawKnownKeyskey(region);
    Map<byte[], byte[]> rawMap = run(new JedisCallback<Map<byte[], byte[]>>() {
      @Override
      public Map<byte[], byte[]> execute(Jedis jedis) {
        Map<byte[], byte[]> rawMap = new HashMap<byte[], byte[]>();

        int offset = 0;
        boolean finished = false;

        do {
          // need to paginate the keys
          Set<byte[]> rawKeys = jedis.zrange(rawKnownKeysKey, (offset) * PAGE_SIZE, (offset + 1) * PAGE_SIZE - 1);
          finished = rawKeys.size() < PAGE_SIZE;
          offset++;
          if (!rawKeys.isEmpty()) {
            List<byte[]> regionedKeys = new ArrayList<byte[]>();
            for (byte[] rawKey : rawKeys) {
              regionedKeys.add(getRegionedKey(rawRegion, rawKey));
            }
            List<byte[]> rawValues = jedis.mget(regionedKeys.toArray(new byte[regionedKeys.size()][]));
            for (int i = 0; i < regionedKeys.size(); i++) {
              byte[] regionedKey = regionedKeys.get(i);
              byte[] rawKey = getRawKey(regionedKey, rawRegion);

              rawMap.put(rawKey, rawValues.get(i));
            }
          }
        } while (!finished);

        return rawMap;
      }
    });

    Map<Object, Object> map = new HashMap<Object, Object>();
    for (Map.Entry<byte[], byte[]> entry : rawMap.entrySet()) {
      Object key = deserializeKey(entry.getKey());
      Object value = deserializeValue(entry.getValue());
      map.put(key, value);
    }
    return map;
  }

  /**
   * multiple get cache items in specified region
   *
   * @param region region name
   * @param keys   cache key collection to retrieve
   * @return cache items
   */
  public List<Object> mget(final String region, final Collection<?> keys) {
    final byte[] rawRegion = rawRegion(region);
    final byte[][] rawKeys = rawKeys(keys);

    List<byte[]> rawValues = run(new JedisCallback<List<byte[]>>() {
      @Override
      public List<byte[]> execute(Jedis jedis) {
        List<byte[]> regionedKeys = new ArrayList<byte[]>();
        for (byte[] rawKey : rawKeys) {
          regionedKeys.add(getRegionedKey(rawRegion, rawKey));
        }
        return jedis.mget(regionedKeys.toArray(new byte[regionedKeys.size()][]));
      }
    });
    return deserializeValues(rawValues);
  }

  /**
   * save cache
   *
   * @param region region name
   * @param key    cache key to save
   * @param value  cache value to save
   */
  public void set(String region, Object key, Object value) {
    set(region, key, value, expiryInSeconds, TimeUnit.SECONDS);
  }

  /**
   * save cache item
   *
   * @param region           region name
   * @param key              cache key to save
   * @param value            cache value to save
   * @param timeoutInSeconds expire timeout in seconds
   */
  public void set(String region, Object key, Object value, long timeoutInSeconds) {
    set(region, key, value, timeoutInSeconds, TimeUnit.SECONDS);
  }

  /**
   * save cache item
   *
   * @param region  region name
   * @param key     cache key to save
   * @param value   cache value to save
   * @param timeout expire timeout
   * @param unit    expire timeout unit
   */
  public void set(final String region, final Object key, final Object value, long timeout, TimeUnit unit) {
    final byte[] rawRegion = rawRegion(region);
    final byte[] rawKey = rawKey(key);
    final byte[] rawValue = rawValue(value);
    final byte[] regionedKey = getRegionedKey(rawRegion, rawKey);

    final int seconds = (int) unit.toSeconds(timeout);

    runWithPipeline(new JedisPipelinedCallback() {
      @Override
      public void execute(Pipeline pipeline) {
        pipeline.set(regionedKey, rawValue);
        if (seconds > 0) {
          pipeline.expire(regionedKey, seconds);
        }

        if (!region.contains("UpdateTimestampsCache")) {
          final byte[] rawKnownKeysKey = rawKnownKeyskey(region);
          long score = System.currentTimeMillis() + seconds * 1000L;
          if (seconds > 0) {
            pipeline.zadd(rawKnownKeysKey, score, rawKey);
          } else {
            pipeline.zadd(rawKnownKeysKey, seconds, rawKey);
          }
//          pipeline.expire(rawKnownKeyskey, seconds);
        }
      }
    });
  }

  /**
   * 获取添加了region的key
   *
   * @param rawRegion
   * @param rawKey
   * @return
   */
  private byte[] getRegionedKey(byte[] rawRegion, byte[] rawKey) {
    byte[] regionedKey = Arrays.copyOf(rawRegion, rawRegion.length + rawKey.length);
    System.arraycopy(rawKey, 0, regionedKey, rawRegion.length, rawKey.length);
    return regionedKey;
  }

  /**
   * 从regionedkey取出rawKey
   *
   * @param regionedKey
   * @param rawRegion
   * @return
   */
  private byte[] getRawKey(byte[] regionedKey, byte[] rawRegion) {
    byte[] rawKey = new byte[regionedKey.length - rawRegion.length];
    System.arraycopy(regionedKey, rawRegion.length, rawKey, 0, rawKey.length);
    return rawKey;
  }

  /**
   * delete cache item in specified region.
   *
   * @param region region name
   * @param key    cache key to delete
   * @return count of deleted key
   */
  public Long del(final String region, final Object key) {
    final byte[] rawRegion = rawRegion(region);
    final byte[] rawKey = rawKey(key);
    final byte[] rawKnownKeysKey = rawKnownKeyskey(region);
    final byte[] regionedKey = getRegionedKey(rawRegion, rawKey);

    runWithPipeline(new JedisPipelinedCallback() {
      @Override
      public void execute(Pipeline pipeline) {
        pipeline.del(regionedKey);
        pipeline.zrem(rawKnownKeysKey, rawKey);
        pipeline.zremrangeByScore(rawKnownKeysKey, 0, System.currentTimeMillis());
      }
    });

    return 1L;
  }

  /**
   * multiplu delete cache items in specified region
   *
   * @param keys key collection to delete
   */
  public void mdel(final String region, final Collection<?> keys) {

    final byte[] rawRegion = rawRegion(region);
    final byte[] rawKnownKeysKey = rawKnownKeyskey(region);
    final byte[][] rawKeys = rawKeys(keys);

    runWithPipeline(new JedisPipelinedCallback() {
      @Override
      public void execute(Pipeline pipeline) {
        List<byte[]> regionedKeys = new ArrayList<byte[]>();
        for (byte[] rawKey : rawKeys) {
          regionedKeys.add(getRegionedKey(rawRegion, rawKey));
        }

        pipeline.del(regionedKeys.toArray(new byte[regionedKeys.size()][]));
        pipeline.zrem(rawKnownKeysKey, rawKeys);
        pipeline.zremrangeByScore(rawKnownKeysKey, 0, System.currentTimeMillis());
      }
    });
  }


  /**
   * delete region
   *
   * @param region region name to delete
   */
  public void deleteRegion(final String region) throws JedisCacheException {
    log.debug("delete region region=[{}]", region);

    final byte[] rawRegion = rawRegion(region);
    final byte[] rawKnownKeysKey = rawKnownKeyskey(region);

    run(new JedisCallback<Long>() {
      @Override
      public Long execute(Jedis jedis) {
        Long count = 0L;
        int offset = 0;
        boolean finished = false;

        do {
          // need to paginate the keys
          Set<byte[]> rawKeys = jedis.zrange(rawKnownKeysKey, (offset) * PAGE_SIZE, (offset + 1) * PAGE_SIZE - 1);
          finished = rawKeys.size() < PAGE_SIZE;
          offset++;
          if (!rawKeys.isEmpty()) {
            List<byte[]> regionedKeys = new ArrayList<byte[]>();
            for (byte[] rawKey : rawKeys) {
              regionedKeys.add(getRegionedKey(rawRegion, rawKey));
            }

            count += jedis.del(regionedKeys.toArray(new byte[regionedKeys.size()][]));
          }
        } while (!finished);

        jedis.del(rawKnownKeysKey);
        return count;
      }
    });
  }

  /**
   * delete cache item which is expired in region,item has set expire,so remove keys expired
   *
   * @param region region name
   */
  public void expire(final String region) {

    try {
      final byte[] rawKnownKeysKey = rawKnownKeyskey(region);
      final long score = System.currentTimeMillis();

      log.debug("delete expired cache item in region[{}] expire time=[{}]", region, score);

      runWithPipeline(new JedisPipelinedCallback() {
        @Override
        public void execute(Pipeline pipeline) {

          pipeline.zremrangeByScore(rawKnownKeysKey, 0, score);
        }
      });

    } catch (Exception ignored) {
      log.warn("Error in Cache Expiration Method.", ignored);
    }
  }

  /**
   * flush db
   */
  public String flushDb() {
    log.info("Flush DB...");

    return run(new JedisCallback<String>() {
      @Override
      public String execute(Jedis jedis) {
        return jedis.flushDB();
      }
    });
  }

  /**
   * serialize cache key
   */
  private byte[] rawKey(final Object key) {
    return keySerializer.serialize(key.toString());
  }

  @SuppressWarnings("unchecked")
  private byte[][] rawKeys(final Collection<?> keys) {
    byte[][] rawKeys = new byte[keys.size()][];
    int i = 0;
    for (Object key : keys) {
      rawKeys[i++] = rawKey(key);
    }
    return rawKeys;
  }

  /**
   * Serialize expiration region name
   */
  private byte[] rawKnownKeyskey(final String region) {
    return rawRegion(region + "~keys");
  }

  /**
   * serializer region name
   */
  private byte[] rawRegion(final String region) {
    return regionSerializer.serialize(region);
  }

  /**
   * deserialize key
   */
  private Object deserializeKey(final byte[] rawKey) {
    return keySerializer.deserialize(rawKey);
  }

  /**
   * serializer cache value
   */
  private byte[] rawValue(final Object value) {
    try {
      return valueSerializer.serialize(value);
    } catch (Exception e) {
      log.warn("value를 직렬화하는데 실패했습니다. value=" + value, e);
      return null;
    }
  }

  /**
   * deserialize raw value
   */
  private Object deserializeValue(final byte[] rawValue) {
    return valueSerializer.deserialize(rawValue);
  }

  /**
   * execute the specified callback
   */
  private <T> T run(final JedisCallback<T> callback) {

    Jedis jedis = jedisPool.getResource();
    try {
      return callback.execute(jedis);
    } finally {
      jedisPool.returnResource(jedis);
    }
  }

  /**
   * execute the specified callback under transaction
   * HINT: https://github.com/xetorthio/org.hibernate.cache.redis.jedis/wiki/AdvancedUsage
   *
   * @param callback executable instance under transaction
   */
  private List<Object> runWithTx(final JedisTransactionalCallback callback) {

    Jedis jedis = jedisPool.getResource();
    try {
      Transaction tx = jedis.multi();
      callback.execute(tx);
      return tx.exec();
    } finally {
      jedisPool.returnResource(jedis);
    }
  }

  /**
   * execute the specified callback under Redis Pipeline
   * HINT: https://github.com/xetorthio/org.hibernate.cache.redis.jedis/wiki/AdvancedUsage
   *
   * @param callback executable instance unider Pipeline
   */
  private void runWithPipeline(final JedisPipelinedCallback callback) {
    final Jedis jedis = jedisPool.getResource();
    try {
      final Pipeline pipeline = jedis.pipelined();
      callback.execute(pipeline);
      // use #sync(), not #exec()
      pipeline.sync();
    } finally {
      jedisPool.returnResource(jedis);
    }
  }

  /**
   * deserize the specified raw key set.
   *
   * @return original key set.
   */
  private Set<Object> deserializeKeys(final Set<byte[]> rawKeys) {
    Set<Object> keys = new HashSet<Object>();
    for (byte[] rawKey : rawKeys) {
      keys.add(deserializeKey(rawKey));
    }
    return keys;
  }

  /**
   * deseriaize the specified raw value collection
   *
   * @return collection of original value
   */
  private List<Object> deserializeValues(final List<byte[]> rawValues) {
    return SerializationTool.deserialize(rawValues, valueSerializer);
  }
}
