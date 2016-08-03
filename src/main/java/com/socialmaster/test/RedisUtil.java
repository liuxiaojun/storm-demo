package com.socialmaster.test;

import java.util.HashMap;
import java.util.Map;
import redis.clients.jedis.Jedis;

/**
 * Created by liuxiaojun on 2016/8/3.
 */
public class RedisUtil {
    private static Map<String, RedisUtil> instanceMap = new HashMap<String, RedisUtil>();
    private Jedis jedis;
    private RedisUtil (String host, int port) {
        this.jedis = new Jedis(host, port);
        System.out.println("connect to redis " + host + " " + port);
    }

    public static synchronized RedisUtil get(String host, int port) {
        RedisUtil instance = instanceMap.get(host + ":" + port);
        if (instance == null) {
            instance = new RedisUtil(host, port);
            instanceMap.put(host + ":" + port, instance);
        }
        return instance;
    }

    public synchronized void hincrBy(String key, String field, long value) {
        boolean ok = false;
        for (int i = 0; i < 5; i++) {
            try {
                Long ret = jedis.hincrBy(key, field, value);
                System.out.println("hincrBy ok for " + key + " " + field + " " + value + " ret " + ret);
                ok = true;
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (!ok) {
            throw new RuntimeException("hincrBy failed for " + key + " " + field + " " + value);
        }
    }
}
