package com.socialmaster.kafka;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by liuxiaojun on 2016/8/5.
 */
public class ParseBolt extends BaseBasicBolt{

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // format: 205.212.115.106 - - [01/Jul/1995:00:00:12 -0400]
        // "GET /shuttle/countdown/countdown.html HTTP/1.0" 200 3985
        String line = tuple.getString(0);
        String[] splits = line.split(" ");
        if (splits.length > 6) {
            String time = splits[3];
            String url = splits[6];
            int index = url.indexOf("?");
            if (index > 0) {
                url = url.substring(0, index);
            }
            collector.emit(new Values(System.currentTimeMillis() / (60 * 1000), url));
        }else{
            System.err.println("can not parse for log " + line);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "url"));
    }
}
