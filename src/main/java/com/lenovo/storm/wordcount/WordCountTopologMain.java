package com.lenovo.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopologMain {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        //1、准备一个TopologyBuilder
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout", new MySpout(), 2);
        topologyBuilder.setBolt("mybolt1", new MySplitBolt(), 2).shuffleGrouping("mySpout");
        topologyBuilder.setBolt("mybolt2", new MyCountBolt(), 4).fieldsGrouping("mybolt1", new Fields("word"));
//        topologyBuilder.setBolt("mybolt2",new MyCountBolt(),4).shuffleGrouping("mybolt1");
        //  config.setNumWorkers(2);
        /**
         * i
         * am
         * lilei
         * love
         * hanmeimei
         */


        //2、创建一个configuration，用来指定当前topology 需要的worker的数量
        Config config = new Config();
        config.setNumWorkers(2);

        //3、提交任务  -----两种模式 本地模式和集群模式
//        StormSubmitter.submitTopology("mywordcount",config,topologyBuilder.createTopology());
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("mywordcount", config, topologyBuilder.createTopology());

    }
}
