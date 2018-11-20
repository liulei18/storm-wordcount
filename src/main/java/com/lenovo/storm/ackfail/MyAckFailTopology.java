package com.lenovo.storm.ackfail;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.lenovo.storm.ackfail.MySpout;

public class MyAckFailTopology {

    public static void main(String[] args) throws Exception {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout",new MySpout(),1);
        topologyBuilder.setBolt("myBolt1",new MyBolt1(),1).shuffleGrouping("mySpout");

        Config conf = new Config();
        String name = MyAckFailTopology.class.getSimpleName();
        if (args!=null && args.length>0) {
            String nimbus = args[0];
            conf.put(Config.NIMBUS_HOST,nimbus);
            conf.setNumWorkers(1);
            StormSubmitter.submitTopology(name,conf,topologyBuilder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name,conf,topologyBuilder.createTopology());
            Thread.sleep(60*60*1000);
            cluster.shutdown();
        }

    }
}
