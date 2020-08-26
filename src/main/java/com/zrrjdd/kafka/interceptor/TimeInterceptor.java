package com.zrrjdd.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 时间戳拦截器
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {

    /**
     * Producer 确保在消息被序列化以及计算分区前调用改方法。
     *
     * @param producerRecord
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        // 创建一个新的 record ，把时间戳写入消息体的最前部
        return new ProducerRecord<String, String>(producerRecord.topic(), producerRecord.partition(), producerRecord.timestamp(), producerRecord.key(), System.currentTimeMillis() + "," + producerRecord.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
