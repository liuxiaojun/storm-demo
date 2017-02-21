package com.socialmaster.demo.kafkaRedis;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by liuxiaojun on 2017/2/21.
 */
public class LogTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        String topicName = "topic-h";
        String zkConnString = "101.201.101.181:2181,123.56.79.218:2181,101.201.100.168:2181";
        BrokerHosts hosts = new ZkHosts(zkConnString);
        String name = LogTopology.class.getSimpleName();
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/kafka/storm", name);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout("kafka_spout", kafkaSpout, 3);

        builder.setBolt("parse", new ParseBolt(), 3).localOrShuffleGrouping("kafka_spout");
        builder.setBolt("conversion", new ConversionBolt(), 3).localOrShuffleGrouping("parse");
        builder.setBolt("count", new CountBolt(), 6).fieldsGrouping("conversion", new Fields("minute","cityCode"));

        Config conf = new Config();
        conf.setMaxSpoutPending(5000);

        conf.setNumWorkers(3);
        StormSubmitter.submitTopologyWithProgressBar("log", conf, builder.createTopology());
    }
}
