package com.socialmaster.kafka;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.UUID;

/**
 * Created by liuxiaojun on 2016/8/5.
 */
public class TopMain {
    public static void main(String[] args) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();

        String topicName = "test-log";
        String zkConnString = "localhost:2181";
        BrokerHosts hosts = new ZkHosts(zkConnString);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout("kafka_spout", kafkaSpout, 2);

        builder.setBolt("parse", new ParseBolt(), 4).shuffleGrouping("kafka_spout");
        builder.setBolt("count", new CountBolt(), 8).fieldsGrouping("parse", new Fields("time", "url"));

        Config conf = new Config();
        conf.setNumWorkers(3);
        StormSubmitter.submitTopologyWithProgressBar(args[0] + "_pv", conf, builder.createTopology());
    }
}
