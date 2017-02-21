package com.socialmaster.tool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by liuxiaojun on 2017/2/21.
 */
public class RedisUtil {
    private static JedisPool pool;
    private static RedisUtil redisUtil ;

    private RedisUtil() {
        this.pool = new JedisPool(new JedisPoolConfig(), "10.10.1.1", 6378, 10000, "1234567xxxx");
    }

    public static synchronized  RedisUtil getInstance() {
        if (redisUtil == null) {
            redisUtil = new RedisUtil();
        }
        return redisUtil;
    }

    public static synchronized void toRedisOnline(String key, String count, Integer ttl) {
        for (int i = 0; i < 5; i++) {
            try {
                Jedis jedis = pool.getResource();
                jedis.set(key, count);
                jedis.expire(key, ttl);
                break;
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
