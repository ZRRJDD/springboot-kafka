package com.zrrjdd.kafka.consumer;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.zrrjdd.kafka.model.Person;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class PersonConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","192.168.117.128:9092");
        props.put("group.id","test");
        props.put("enable.auto.commit","false"); // 关闭自动提交offset
//        props.put("auto.commit.interval.ms","1000");

        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(props);
        String topic = "person-topic";
        consumer.subscribe(Arrays.asList(topic));
        while (true){
            ConsumerRecords<String,String> records = consumer.poll(Long.MAX_VALUE);
            for (ConsumerRecord<String,String> record:records){
                System.out.printf("offset = %d , key = %s ,value = %s%n",record.offset(),record.key(),record.value());
                JSONObject jsonObject = JSONUtil.parseObj(record.value());
                Person p = new Person(jsonObject.getStr("name"),jsonObject.getInt("age"),jsonObject.getDate("birth"));
                System.out.println(p.toString());
            }

            // 异步提交
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    if (e != null){
                        System.out.println("Commit failed for " + map);
                    }
                }
            });
        }
    }

}
