package com.socialmaster.demo.kafkaRedis;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by liuxiaojun on 2017/2/21.
 */

public class ParseBolt extends BaseBasicBolt {
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String line = tuple.getString(0);
        String[] splits = line.split(",");
        if (splits.length >= 3) {
            Long minute = Long.valueOf(splits[0].substring(0, 12));
            String apmac = splits[2];
            collector.emit(new Values(minute, apmac));
        }else{
            System.err.println("can not parse for log " + line);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("minute", "apMac"));
    }
}
