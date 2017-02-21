package com.socialmaster.spout.join;

import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.tuple.Fields;

/**
 * Created by liuxiaojun on 2017/2/21.
 */
public class JoinTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("log-vspout", new VSpout(), 1);
        builder.setSpout("log-bspout", new BSpout(), 1);

        builder.setBolt("log-merge", new MergeBolt(), 2)
                .fieldsGrouping("log-vspout", "visit", new Fields("user"))
                .fieldsGrouping("log-bspout", "business", new Fields("user"));

        builder.setBolt("log-stat", new LogStatBolt(), 2)
                .fieldsGrouping("log-merge", new Fields("srcid"));

        Config conf = new Config();

        //实时计算不需要可靠的消息，故关闭acker节省通信资源
        conf.setNumAckers(0);
        conf.setNumWorkers(1);
        StormSubmitter.submitTopology(
                "spout-join",
                conf,
                builder.createTopology()
        );
    }
}
