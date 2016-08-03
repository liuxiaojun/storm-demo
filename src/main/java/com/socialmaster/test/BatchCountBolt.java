package com.socialmaster.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.task.TopologyContext;

import redis.clients.jedis.Jedis;

import java.util.Date;
import java.text.SimpleDateFormat;

/**
 * Created by liuxiaojun on 16/8/3.
 */
public class BatchCountBolt extends BaseBasicBolt{
    private Jedis jedis;
    TreeMap<Long, Map<String, Integer>> timeCounts = new TreeMap<Long, Map<String, Integer>>();

    private void output(String minute, String url, String count) {
        for (int i = 0; i < 5; i++) {
            try {
                jedis.hset(minute, url, count);
                jedis.expire(minute,24*60*60);
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        String host = "10.11.0.3";
        Integer port = 6379;
        System.out.println("connecting to redis " + host + ":" + port);
        this.jedis = new Jedis(host, port);
        System.out.println("connected to redis " + host + ":" + port);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //long currentMinute = System.currentTimeMillis() / (60 * 1000);
        Date nowTime = new Date();
        SimpleDateFormat time = new SimpleDateFormat("yyyyMMddHHmm");
        Long currentMinute = Long.valueOf(time.format(nowTime));

        if (tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            Iterator<Map.Entry<Long, Map<String, Integer>>> iter = timeCounts.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, Map<String, Integer>> entry = iter.next();
                long minute = entry.getKey();
                if (currentMinute > minute + 1) {
                    for (Map.Entry<String, Integer> counts : entry.getValue().entrySet()) {
                        String url = counts.getKey();
                        Integer count = counts.getValue();
                        output(String.valueOf(minute), url, String.valueOf(count));
                    }
                    iter.remove();
                } else {
                    break;
                }
            }
            return;
        }else {
            System.out.println("input tuple is " + tuple);
            Long minute = tuple.getLong(0);
            String url = tuple.getString(1);

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
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
        return conf;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("minute", "url", "count"));
    }
}
