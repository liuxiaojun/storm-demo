package com.socialmaster.test;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.FileWriter;
import java.util.Map;

/**
 * Created by liuxiaojun on 16/2/27.
 */
public class ParseBolt extends BaseBasicBolt{
    /**
     * 初始化的方法,和open类似,被调用一次
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // 从消息远足tuple 中拿到上一个组件发送过来的数据
        Long minute = tuple.getLong(0);
        String line = tuple.getString(1);
        String[] splits = line.split("_");
        String url = splits[0];
        String ua = splits[1]; // 这个不用
        basicOutputCollector.emit(new Values(minute,url));
    }

    /**
     * 声明该组建要发送出去的tuple的字段定义, 这里直接写文件就不用输出了
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("minute","url"));
    }
}
