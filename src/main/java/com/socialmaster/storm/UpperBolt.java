package com.socialmaster.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by liuxiaojun on 16/2/27.
 * 将收到的原始的商品名转换成大些发送出去
 */
public class UpperBolt extends BaseBasicBolt{

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // 从tuple中拿到我们的数据 -- 原始商品名
        String src_word = tuple.getString(0);
        // 转换成大些
        String upper_word = src_word.toUpperCase();
        // 发送出去
        collector.emit(new Values(upper_word));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //声明输出字段的名称
        declarer.declare(new Fields("upper_word"));
    }
}
