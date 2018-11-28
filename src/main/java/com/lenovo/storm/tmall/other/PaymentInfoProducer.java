package com.lenovo.storm.tmall.other;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Describe: 随机生产订单消息，此服务单独部署
 */
public class PaymentInfoProducer {

    private final static String TOPIC = "paymentInfo";
    public static void main(String[] args) throws InterruptedException {
        // 设置配置信息
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list",
                "mini2:9092,mini3:9092,mini4:9092");
        props.put("request.required.acks", "1");
        // 创建producer
        Producer<Integer, String> producer = new Producer<Integer, String>(new ProducerConfig(props));
        // 发送数据
        int messageNo = 1;
        while (true) {
            Thread.sleep(1000);
            producer.send(new KeyedMessage<Integer, String>(TOPIC, PaymentInfoProducer.genPaymentInfo()));
            messageNo++;
        }
    }

    private static String genPaymentInfo(){
        return new PaymentInfo().random();
    }
}
