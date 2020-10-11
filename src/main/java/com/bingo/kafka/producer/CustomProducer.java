package com.bingo.kafka.producer;

import com.bingo.kafka.interceptor.CounterInterceptor;
import com.bingo.kafka.interceptor.TimeInterceptor;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

/**
 * @author: jiangjiabin
 * @date: Create in 22:50 2020/10/11
 * @description: 异步API
 */
public class CustomProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "bingo100:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //拦截器
        List<String> interceptors = Lists.newArrayList();
        interceptors.add(TimeInterceptor.class.getName());
        interceptors.add(CounterInterceptor.class.getName());
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);


        //创建一个生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //调用send方法
        for (int i = 0; i < 1000; i++) {
            producer.send(new ProducerRecord<>("first_topic", i + "", "message--" + i));
        }

        //关闭生产者
        producer.close();

    }
}
