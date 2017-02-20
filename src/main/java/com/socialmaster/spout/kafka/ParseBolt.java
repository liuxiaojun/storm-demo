package com.socialmaster.spout.kafka;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by liuxiaojun on 2017/2/20.
 */
public class ParseBolt extends BaseBasicBolt{
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

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "url"));
    }
}
