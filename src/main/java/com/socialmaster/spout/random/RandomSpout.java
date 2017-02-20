package com.socialmaster.spout.random;

import java.util.Map;
import java.util.Random;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Created by liuxiaojun on 16/2/27.
 */
public class RandomSpout extends BaseRichSpout {
    SpoutOutputCollector collector = null;
    String[] goods={"iphone", "xiaomi", "huawei", "zhongxing", "meizu"};

    /**
     * 获取消息并发送给下一个组件的方法,会被Storm不断的调用
     * 从goods 数据中随机取一个商品名称封装到tuple中发送出去
     */
    public void nextTuple() {
        Random random = new Random();
        String good = goods[random.nextInt(goods.length)];

        //封装到tuple发送出去
        collector.emit(new Values(good));
    }

    /**
     * 进行初始化,只在开始的时候调用一次
     */
    public void open(Map conf, TopologyContext content, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 定义tuple的scheme
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("src_word"));
    }
}
