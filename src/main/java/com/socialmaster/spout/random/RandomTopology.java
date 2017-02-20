package com.socialmaster.spout.random;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by liuxiaojun on 2017/2/20.
 */
public class RandomTopology {
    public static void main(String[] args) throws  AuthorizationException, AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        // 设置消息源组件为 Randomspout
        topologyBuilder.setSpout("randomspout", new RandomSpout(), 4);

        // 设置逻辑源组件为 UpperBolt, 并指定接收 randomspout 的消息
        topologyBuilder.setBolt("upperblot", new UpperBolt(), 4).shuffleGrouping("randomspout");

        // 设置逻辑处理组件为 SuffixBolt, 并指定接收 upperblot 的消息
        topologyBuilder.setBolt("shuffix", new SuffixBolt(), 4).shuffleGrouping("upperblot");

        // 创建一个topology
        StormTopology topo = topologyBuilder.createTopology();

        // 创建一个storm配置参数对象
        Config config = new Config();
        config.setNumWorkers(4);   // 为这个topo 启动的进程数
        config.setDebug(true);
        config.setNumAckers(0);

        // 提交topo到storm集群中
        StormSubmitter.submitTopology("demotopo", config, topo);
    }
}
