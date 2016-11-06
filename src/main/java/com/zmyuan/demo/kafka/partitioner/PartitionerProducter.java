package com.zmyuan.demo.kafka.partitioner;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by zdb on 2016/10/11.
 */
public class PartitionerProducter {

    private Producer<String, String> producer;

    private static String brokerList = "192.168.0.150:9092";

    public PartitionerProducter() {
        Properties props = new Properties();

        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", StringEncoder.class.getName());
        props.put("partitioner.class", "com.zmyuan.demo.kafka.partitioner.RandomPartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        producer = new Producer<String, String>(config);
    }


    void sendOne(String topic) {

        List<KeyedMessage<String, String>> messages = new ArrayList<KeyedMessage<String, String>>();

        messages.add(new KeyedMessage<String, String>(topic, "1", "msg1"));
        messages.add(new KeyedMessage<String, String>(topic, "2", "msg2"));
        messages.add(new KeyedMessage<String, String>(topic, "1", "msg3"));
        messages.add(new KeyedMessage<String, String>(topic, "3", "msg4"));
        messages.add(new KeyedMessage<String, String>(topic, "4", "msg5"));
        messages.add(new KeyedMessage<String, String>(topic, "3", "msg6"));

        producer.send(messages);
    }
}
