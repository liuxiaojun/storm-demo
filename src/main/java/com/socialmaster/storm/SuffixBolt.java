package com.socialmaster.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Created by liuxiaojun on 16/2/27.
 * 给商品名称添加后缀,然后将数据写入文件中
 */
public class SuffixBolt extends BaseBasicBolt{

    FileWriter fileWriter = null;
    /**
     * 初始化的方法,和open类似,被调用一次
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        try {
            fileWriter = new FileWriter("/Users/liuxiaojun/Document/storm-tmp-file/"+ UUID.randomUUID());
        } catch (IOException e) {
            e.printStackTrace();
        }
        //super.prepare(stormConf, context);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // 从消息远足tuple 中拿到上一个组件发送过来的数据
        String upper_word = tuple.getString(0);

        // 给商品名称添加后缀
        String result = upper_word + "_shuffix";
        try {
            fileWriter.append(result);
            fileWriter.append("\n");
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 声明该组建要发送出去的tuple的字段定义, 这里直接写文件就不用输出了
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
