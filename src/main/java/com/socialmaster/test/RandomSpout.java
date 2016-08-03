package com.socialmaster.test;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

import java.util.Date;
import java.text.SimpleDateFormat;

/**
 * Created by liuxiaojun on 2016/8/3.
 */
public class RandomSpout extends BaseRichSpout {

    SpoutOutputCollector collector = null;
    String[] urls ={"www.baidu.com", "www.taobao.com", "www.mi.com", "www.115it.com", "www.meizu.com","www.qq.com"};

    /**
     * 获取消息并发送给下一个组件的方法,会被Storm不断的调用
     * 从goods 数据中随机取一个商品名称封装到tuple中发送出去
     */
    public void nextTuple() {
        Random random = new Random();
        //Long minute = System.currentTimeMillis()/(1000*60);
        Date nowTime = new Date();
        SimpleDateFormat time = new SimpleDateFormat("yyyyMMddHHmm");
        Long minute = Long.valueOf(time.format(nowTime));
        String url = urls[random.nextInt(urls.length)];
        //封装到tuple发送出去
        collector.emit(new Values(minute, url+"_ua"));
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
        declarer.declare(new Fields("minute","url_ua"));
    }
}

