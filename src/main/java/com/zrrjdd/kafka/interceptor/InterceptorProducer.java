package com.zrrjdd.kafka.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class InterceptorProducer {

    public static void main(String[] args) {

        //1.设置配置信息
        Properties props = new Properties();
        props.put("bootstrap.servers","192.168.117.128:9092");
        props.put("acks","all");
        props.put("retries",3);
        props.put("batch.size",16384);
        props.put("linger.ms",1); //等待时间
        props.put("buffer.memory",33554432);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        //2. 构建拦截链
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.zrrjdd.kafka.interceptor.TimeInterceptor");
        interceptors.add("com.zrrjdd.kafka.interceptor.CounterInterceptor");

        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        //3.发送消息
        String topic = "lowrisk-topic";
        Producer<String,String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 67; i++) {
            ProducerRecord<String,String> record = new ProducerRecord<>(topic,"message"+i);
            producer.send(record);
        }


        //4.一定要关闭producer，这样才会调用interceptor的close 方法
        producer.close();



    }




}
