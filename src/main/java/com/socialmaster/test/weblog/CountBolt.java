package com.socialmaster.test.weblog;

import redis.clients.jedis.Jedis;
import java.util.Date;
import java.util.Iterator;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;

/**
 * Created by liuxiaojun on 2017/2/21.
 */
public class CountBolt extends BaseBasicBolt {

    TreeMap<Long, Map<String, Integer>> timeCounts = new TreeMap<Long, Map<String, Integer>>();
    private Jedis jedis;
    private String prefix;

    private void output(long minute, String url, Integer count) {
        for (int i = 0; i < 5; i++) {
            try {
                jedis.hset(prefix, new Date(minute * 60 * 1000) + "_" + url,
                        count.toString());
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void prepare(Map stormConf, TopologyContext context) {
        String host = "localhost";
        Integer port = 6379;
        if (stormConf.get("redis.host") != null) {
            host = (String) stormConf.get("redis.host");
        }
        if (stormConf.get("redis.port") != null) {
            port = (Integer) stormConf.get("redis.port");
        }
        System.out.println("connecting to redis " + host + ":" + port);
        this.jedis = new Jedis(host, port);
        System.out.println("connected to redis " + host + ":" + port);
        this.prefix = (String) stormConf.get("topology.name");
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        long currentMinute = System.currentTimeMillis() / (60 * 1000);

        // handle timer tick
        if (tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            // save counts
            Iterator<Map.Entry<Long, Map<String, Integer>>> iter = timeCounts
                    .entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, Map<String, Integer>> entry = iter.next();
                long minute = entry.getKey();
                if (currentMinute > minute + 1) {
                    for (Map.Entry<String, Integer> counts : entry.getValue().entrySet()) {
                        String url = counts.getKey();
                        Integer count = counts.getValue();
                        output(minute, url, count);
                    }

                    iter.remove();
                } else {
                    break;
                }
            }
            return;
        }

        long minute = tuple.getLong(0);
        String url = tuple.getString(1);

        if (currentMinute > minute + 1) {
            System.out.println("drop outdated tuple " + tuple);
            return;
        }

        Map<String, Integer> counts = timeCounts.get(minute);
        if (counts == null) {
            counts = new HashMap<String, Integer>();
            timeCounts.put(minute, counts);
        }
        Integer count = counts.get(url);
        if (count == null)
            count = 0;
        count++;
        counts.put(url, count);
        collector.emit(new Values(minute, url, count));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "url", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
        return conf;
    }
}
