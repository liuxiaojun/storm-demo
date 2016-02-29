package com.socialmaster.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by liuxiaojun on 16/2/27.
 */
public class RandomSpout extends BaseRichSpout{

    SpoutOutputCollector collector = null;
    String[] goods={"iphone", "xiaomi", "huawei", "zhongxing", "meizu"};

    /**
     * 获取消息并发送给下一个组件的方法,会被Storm不断的调用
     * 从goods 数据中随机取一个商品名称封装到tuple中发送出去
     */
    @Override
    public void nextTuple() {
        Random random = new Random();
        String good = goods[random.nextInt(goods.length)];

        //封装到tuple发送出去
        collector.emit(new Values(good));
    }

    /**
     * 进行初始化,只在开始的时候调用一次
     */
    @Override
    public void open(Map conf, TopologyContext content, SpoutOutputCollector collector) {
        this.collector = collector;

    }

    /**
     * 定义tuple的scheme
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("src_word"));
    }
}
