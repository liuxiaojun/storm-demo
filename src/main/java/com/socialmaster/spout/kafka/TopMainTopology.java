package com.socialmaster.spout.kafka;

import java.util.UUID;
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

import com.socialmaster.spout.kafka.ParseBolt;
import com.socialmaster.spout.kafka.CountBolt;
/**
 * Created by liuxiaojun on 2017/2/20.
 */
public class TopMainTopology {
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
