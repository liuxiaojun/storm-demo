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
public class VSpout extends BaseRichSpout {
    private SpoutOutputCollector _collector;
    private String[] _users = {"userA", "userB", "userC", "userD", "userE"};
    private String[] _srcid = {"s1", "s2", "s3", "s1", "s1"};
    private int count = 5;

    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    public void nextTuple() {
        for (int i=0; i<count; i++){
            try {
                Thread.sleep(1000);
                _collector.emit(
                        "visit",
                        new Values(
                                System.currentTimeMillis(),
                                _users[i],
                                _srcid[i]
                        )
                );
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                "visit",
                new Fields("time", "user", "srcid")
        );
    }
}
