package com.zmyuan.demo.kafka.replica;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;

/**
 * Created by zdb on 2016/10/23.
 */
public class ProducterTest {

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("metadata.broker.list", "192.168.0.150:9092");
        props.put("serializer.class", StringEncoder.class.getName());
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);
        String topic = "t1";

        producer.send(new KeyedMessage<String, String>(topic, "1", "msg1"));
        producer.close();
    }

}
