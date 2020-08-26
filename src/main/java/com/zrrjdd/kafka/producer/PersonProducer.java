package com.zrrjdd.kafka.producer;

import cn.hutool.json.JSONUtil;
import com.zrrjdd.kafka.model.Person;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

public class PersonProducer {

    public static void main(String[] args) {
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

        String topic = "person-topic";

        for (int i = 0; i < 20; i++) {
            Person p = new Person("name"+i,i,new Date());
            producer.send(new ProducerRecord<String,String>(topic, JSONUtil.toJsonStr(p)));
        }
        producer.close();
        System.out.println("close");
    }


}
