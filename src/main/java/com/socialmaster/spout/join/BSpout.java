package com.socialmaster.spout.join;

import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Created by liuxiaojun on 2017/2/21.
 */
public class BSpout extends BaseRichSpout{
    private SpoutOutputCollector _collector;
    private String[] _users = {"userA", "userB", "userC", "userD", "userE"};
    private String[] _pays = {"100", "200", "300", "400", "500"};
    private int count = 5;

    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    public void nextTuple() {
        for (int i=0; i<count; i++){
            try {
                Thread.sleep(1500);
                _collector.emit(
                        "business",
                        new Values(
                                System.currentTimeMillis(),
                                _users[i],
                                _pays[i]
                        )
                );
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                "business",
                new Fields("time", "user", "pay")
        );
    }
}
