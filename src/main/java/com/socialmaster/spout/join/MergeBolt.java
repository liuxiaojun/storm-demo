package com.socialmaster.spout.join;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by liuxiaojun on 2017/2/21.
 */
public class MergeBolt extends BaseRichBolt {
    private transient OutputCollector _collector;
    //暂时存储用户的来源记录
    private HashMap<String, String> srcmap;

    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        if (srcmap == null) {
            srcmap = new HashMap<String, String>();
        }
    }

    public void execute(Tuple input) {
        String streamID = input.getSourceStreamId();
        if (streamID.equals("visit")) {
            String user = input.getStringByField("user");
            String srcid = input.getStringByField("srcid");
            srcmap.put(user, srcid);
        } else if (streamID.equals("business")) {
            String user = input.getStringByField("user");
            String pay = input.getStringByField("pay");
            String srcid = srcmap.get(user);

            if (srcid != null) {
                _collector.emit(new Values(user, pay, srcid));
                srcmap.remove(user);
            } else{
                // 一般只有成交日志快于流量日志时才会发生
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user", "pay", "srcid"));
    }
}
