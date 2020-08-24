package com.zrrjdd.kafka.producer;


import org.apache.kafka.clients.producer.*;
import org.junit.Test;

import java.util.Properties;

/**
 *  消息发送流程：
 *      Kafka 的Producer发送消息采用的是 异步发送 的方式。在消息发送过程中，涉及到了两个线程 --- main线程 和 Sender线程 ，以及一个线程共享变量 --- RecordAccumulator。
 *  mian 线程 将消息发送给RecordAccumulator ，Sender 线程 不断从 RecordAccumulator 中拉取消息发送到 Kafka broker
 *
 *
 *  异步发送API
 *
 *  需要用到的类：
 *  KafkaProducer： 需要创建一个生产者对象，用来发送数据
 *  ProducerConfig： 获取所需的一系列配置参数
 *  ProducerRecord： 每条数据 都封装成一个ProducerRecord 对象
 *
 *
 */
public class CustomProducer1 {

    /**
     * 2.带回调函数的API
     *
     * 回调函数 会在 producer 收到 ack 时 调用，该方法有两个参数，分别时 `RecordMetadata` 和 `Exception`, 如果Exception为null，说明消息发送成功，如果Exception不为null，说明消息发送失败
     *
     * 注意： 消息发送失败会自动重试，不需要我们在回调函数中手动重试
     */
    @Test
    public void testCallbackSend(){
        Properties props = new Properties();
        props.put("bootstrap.servers","192.168.117.128:9092"); //kafka集群，broker-list
        props.put("acks","all"); // acks 确认机制
        props.put("retries",1);  //重试次数
        props.put("batch.size",16384); // 批次大小
        props.put("linger.ms",1);//等待时间
        props.put("buffer.memory",33554432);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String,String>("lowrisk-topic",Integer.toString(i),Integer.toString(i)),new Callback(){
                // 回调函数，该方法会在Producer 收到ack时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null){
                        System.out.println("success -> "+recordMetadata.offset() +"  "+recordMetadata.topic() + "  "+ recordMetadata.partition());
                    } else {
                        e.printStackTrace();
                    }
                }
            });
        }
        producer.close();
    }

    /**
     *  1.不带回调函数的API
     */
    @Test
    public void testNoCallbackSend(){
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
            producer.send(new ProducerRecord<String,String>("lowrisk-topic",Integer.toString(i),Integer.toString(i)));
        }
        producer.close();
        System.out.println("close");
    }

}
