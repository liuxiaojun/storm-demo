package com.socialmaster.spout.random;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseBasicBolt;

/**
 * Created by liuxiaojun on 2017/2/20.
 */
public class SuffixBolt extends BaseBasicBolt {

    FileWriter fileWriter = null;
    /**
     * 初始化的方法,和open类似,被调用一次
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            fileWriter = new FileWriter("/tmp/storm-tmp-file-"+ UUID.randomUUID());
        } catch (IOException e) {
            e.printStackTrace();
        }
        //super.prepare(stormConf, context);
    }

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
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
