package com.socialmaster.demo.kafkaRedis;


import java.util.*;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.socialmaster.tool.HdfsFileUtil;

/**
 * Created by liuxiaojun on 2017/2/21.
 */
public class ConversionBolt extends BaseBasicBolt {
    private static Map<String, String> apCity = new HashMap<String, String>();
    public void prepare(Map stormConf, TopologyContext context) {
        initData();
    }

    public void initData(){
        this.apCity = HdfsFileUtil.readHdfsFileToMap("/data/user/ap_city.txt");
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) &&
                tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            initData();
            return;
        }
        long minute = tuple.getLongByField("minute");
        String apMac = tuple.getStringByField("apMac");
        if (apCity.containsKey(apMac)){
            String cityCode = apCity.get(apMac);
            collector.emit(new Values(minute, cityCode, apMac));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("minute", "cityCode", "apMac"));
    }

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3600);
        return conf;
    }
}
