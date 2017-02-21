package com.socialmaster.trident.stream;

import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.spout.IBatchSpout;

/**
 * Created by liuxiaojun on 2017/2/21.
 */
public class Test {
    public static void main(String[] args) {
        IBatchSpout iBatchSpout = null;
        TridentTopology tridentTopology = new TridentTopology();
        Stream stream = tridentTopology.newStream("Stream",iBatchSpout);
    }
}
