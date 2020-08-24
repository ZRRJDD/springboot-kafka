package com.zrrjdd.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 *  Consumer API
 *
 *      Consumer 消费数据时的可靠性时很容易保证的，因为数据在Kafka 中时持久化的，故不用担心数据丢失问题。
 *
 *      由于consumer 在消费过程中可能会出现断电宕机等故障，consumer恢复后，需要从故障前的位置继续消费，所以consumer需要实时记录自己消费到了哪一个offset，以便故障恢复后继续消费。
 *
 *      所以offset 的维护是Consumer 消费数据必须考虑的问题。
 *
 *  需要用到的类：
 *
 *      KafkaConsumer：需要创建一个消费者对象，用来消费数据
 *      ConsumerConfig： 获取所需的一系列配置参数
 *      ConsumerRecord： 每条数据都要封装成一个ConsumerRecord对象
 *
 */
public class CustomConsumer {

    /**
     *  自动提交offset
     *
     *  为了使我们能够专注于自己的业务逻辑，Kafka提供了自动提交offset 的功能，自动提交offset 的相关参数
     *
     *  enable.auto.commit： 是否开启自动提交offset的功能
     *  enable.commit.interval.ms : 自动提交 offset 的时间间隔
     *
     */
    @Test
    public void testAutoOffset(){
        Properties props = new Properties();
        props.put("bootstrap.servers","192.168.117.128:9092");
        props.put("group.id","test");
        props.put("enable.auto.commit","true");
        props.put("auto.commit.interval.ms","1000");

        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(props);
        consumer.subscribe(Arrays.asList("lowrisk-topic"));
        while (true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record:records){
                System.out.printf("offset = %d , key = %s ,value = %s%n",record.offset(),record.key(),record.value());
            }
        }
    }

    /**
     *  同步提交 offset
     *
     *   由于同步提交offset有失败重试机制，故更加可靠，以下为同步提交offset的示例
     */
    @Test
    public void testCommitSyncOffset(){
        Properties props = new Properties();
        props.put("bootstrap.servers","192.168.117.128:9092");
        props.put("group.id","test");
        props.put("enable.auto.commit","false"); // 关闭自动提交offset
//        props.put("auto.commit.interval.ms","1000");

        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(props);
        consumer.subscribe(Arrays.asList("lowrisk-topic"));
        while (true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record:records){
                System.out.printf("offset = %d , key = %s ,value = %s%n",record.offset(),record.key(),record.value());
            }

            // 同步提交，当前线程会阻塞知道offset提交成功
            consumer.commitSync();
        }
    }

    /**
     *  异步提交 offset
     *
     *      虽然同步提交offset更可靠一些，但是由于其会阻塞当前线程，直到提交成功。因此 吞吐量会受到很大的影响。因此更多情况下，会选用异步提交offset的方式。
     *
     */
    @Test
    public void testCommitAsyncOffset(){
        Properties props = new Properties();
        props.put("bootstrap.servers","192.168.117.128:9092");
        props.put("group.id","test");
        props.put("enable.auto.commit","false"); // 关闭自动提交offset
//        props.put("auto.commit.interval.ms","1000");

        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(props);
        consumer.subscribe(Arrays.asList("lowrisk-topic"));
        while (true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for (ConsumerRecord<String,String> record:records){
                System.out.printf("offset = %d , key = %s ,value = %s%n",record.offset(),record.key(),record.value());
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
