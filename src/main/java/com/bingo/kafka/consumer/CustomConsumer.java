package com.bingo.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * @author: jiangjiabin
 * @date: Create in 23:37 2020/10/11
 * @description:
 */
public class CustomConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "bingo100:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bingo");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("first_topic"), new ConsumerRebalanceListener() {

            //提交当前负责分区的offset (可不用写)
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("==============回收的分区================");
                for (TopicPartition partition : partitions) {
                    System.out.println("partition: " + partition);
                }
            }

            //定位新分配的分区的offset
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    System.out.println("partition: " + partition);
                    Long offset = getPartitionOffset(partition);
                    consumer.seek(partition, offset);
                }
            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                System.out.println("topic = " + record.topic() + " , key = " + record.key()
                        + " , offset = " + record.offset()
                        + " , value = " + record.value());
                TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                commitOffset(topicPartition, record.offset() + 1L);
            }
        }
    }

    private static void commitOffset(TopicPartition topicPartition, Long offset) {
        //todo... 存offset  例如redis
    }

    /**
     * 从存储offset的地方获取，例如redis
     *
     * @param partition
     * @return
     */
    private static Long getPartitionOffset(TopicPartition partition) {

        return 0L;

    }
}
