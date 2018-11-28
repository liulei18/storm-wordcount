package com.lenovo.storm.tmall;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

public class PaymentInfoSpout2 extends BaseRichSpout {
    private static final String TOPIC = "paymentInfo";
    private Properties props;
    private ConsumerConnector consumer;
    private SpoutOutputCollector collector;
    //ArrayBlockingQueue是一个由数组支持的有界阻塞队列。此队列按 FIFO（先进先出）原则对元素进行排序。
    // 队列的头部 是在队列中存在时间最长的元素
    private ArrayBlockingQueue<String> paymentInfoQueue = new ArrayBlockingQueue<String>(100);

    ConsumerIterator<byte[], byte[]> it;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("paymentInfo"));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        props = new Properties();
        props.put("auto.offset.reset", "smallest"); //必须要加，如果要读旧数据
        props.put("zookeeper.connect", "mini2:2181,mini3:2181,mini4:2181");
        props.put("group.id", "testGroup");
        props.put("zookeeper.session.timeout.ms", "30000");
        props.put("zookeeper.sync.time.ms", "20000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("rebalance.max.retries", "5");
        props.put("rebalance.backoff.ms", "1200");

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(TOPIC).get(0);
        it = stream.iterator();


    }

    public void nextTuple() {
        System.out.println("spout was invoked...");
        try {
            while (it.hasNext()) {
                System.out.println("xiaoxi: " + new String(it.next().message()));
                paymentInfoQueue.add(new String(it.next().message()));
                collector.emit(new Values(paymentInfoQueue.take()));
            }
            try {
                Thread.sleep(1000);
            } catch (Exception e) {

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
