package com.socialmaster.trident.wordCount;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Created by liuxiaojun on 2017/2/19.
 */
public class Job {
    public static void main(String[] args) {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"),3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat")
        );

        // FixedBatchSpout 不断循环生产参数中列出的4个句子，下面声明了TridentTopology对象，并在newStream
        // 方法中引用了FixedBatchSpout

        spout.setCycle(true);
        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout",spout)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(),
                        new Count(),
                        new Fields("count")
                )
                .parallelismHint(6);
    }
}