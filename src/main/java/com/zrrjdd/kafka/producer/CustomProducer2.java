package com.zrrjdd.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 *  同步发送API
 *
 *  同步发送的意思就是，一条消息发送之后，会阻塞当前线程，直至返回ack
 *
 *  由于send方法返回的是一个Future对象，根据Future对象的特点，我们可以实现同步发送的效果，只需要在调用Future 对象的get方法即可。
 *
 */
public class CustomProducer2 {


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        // kafka集群 ,地址
        props.put("bootstrap.servers","192.168.117.128:9092");
        // acks 确认机制
        props.put("acks","all");
        // 重试次数
        props.put("retries",1);
        // 批次大小
        props.put("batch.size",16384);
        //等待时间
        props.put("linger.ms",1);

        // RecordAccumulator 缓冲区大小
        props.put("buffer.memory",33554432);

        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String,String>("lowrisk-topic",0,Integer.toString(i),Integer.toString(i))).get();
        }
        producer.close();
        System.out.println("close");
    }

}
