package com.socialmaster.spout.join;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Created by liuxiaojun on 2017/2/21.
 */
public class LogStatBolt extends BaseRichBolt{
    private transient OutputCollector _collector;
    private HashMap<String, Long> srcpay; //暂时存储用户的来源记录

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        _collector = collector;
        if (srcpay == null) {
            srcpay = new HashMap<String, Long>();
        }
    }

    public void execute(Tuple input) {
        String pay = input.getStringByField("pay");
        String srcid = input.getStringByField("srcid");

        if (srcpay.containsKey(srcid)) {
            srcpay.put(srcid, Long.parseLong(pay) + srcpay.get(srcid));
        } else {
            srcpay.put(srcid, Long.parseLong(pay));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
