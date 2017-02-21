package com.socialmaster.tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by liuxiaojun on 2017/2/21.
 */
public class HdfsFileUtil {
    public static synchronized Map<String, String> readHdfsFileToMap(String filePath) {
        Map<String, String> dataMap = new HashMap<String, String>();
        FileSystem fs = null;
        FSDataInputStream inputStream = null;
        BufferedReader br = null;
        try {
            Configuration conf = new Configuration();
            conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));

            conf.set("fs.defaultFS", "hdfs://nameservice1");
            conf.set("dfs.nameservices", "nameservice1");
            conf.set("dfs.ha.namenodes.nameservice1", "cdh2219,cdh2220");
            conf.set("dfs.namenode.rpc-address.nameservice1.cdh2219", "10.11.0.19:8020");
            conf.set("dfs.namenode.rpc-address.nameservice1.cdh2220", "10.11.0.20:8020");
            conf.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

            fs = FileSystem.get(conf);
            inputStream = fs.open(new Path(filePath));
            br = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] strArr = line.split(",");
                if (strArr.length == 2) {
                    dataMap.put(strArr[0], strArr[1]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (inputStream != null) {
                    inputStream.close();
                }
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return dataMap;
    }
}
