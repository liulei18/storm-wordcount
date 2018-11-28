package com.lenovo.storm.tmall;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import com.lenovo.storm.tmall.other.PaymentInfo;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public class FilterMessageBlot extends BaseBasicBolt {
    public void execute(Tuple input, BasicOutputCollector collector) {
        //读取订单数据 {"orderId":"a54e6b99846b4ff0a3eb4b2d6b9b46f0","createOrderTime":"Nov 11, 2015 12:22:12 PM","paymentId":"f1aeb4cfebd64d2bada562d747c02a57","productPrice":287,"promotionPrice":49,"payPrice":248,"num":0}
        String paymentInfoStr = input.getStringByField("paymentInfo");
        //将订单数据解析成JavaBean
        PaymentInfo paymentInfo = new Gson().fromJson(paymentInfoStr, PaymentInfo.class);
        // 过滤订单时间,如果订单时间在2015.11.11这天才进入下游开始计算
        Date date = paymentInfo.getCreateOrderTime();
        /*if (Calendar.getInstance().get(Calendar.DAY_OF_MONTH) != 31) {
            return;
        }*/
        Calendar calendar = Calendar.getInstance();//日历对象
        calendar.setTime(date);//设置当前日期
        if (calendar.get(Calendar.DATE) != 11) {
            return;
        }
        collector.emit(new Values(paymentInfoStr));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("message"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }
}
