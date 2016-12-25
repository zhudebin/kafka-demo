package com.zmyuan.demo.kafka10;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

/**
 * Created by zdb on 2016/11/27.
 */
public class DemoConsumer {

    private static String topic = "topic1";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka0:9092");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("group.id", "g1");
        props.put("client.id", "c1");
        props.put("buffer.memory", 33443332);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("key.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {

            }
        });

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            System.out.println(records.count());
            Set<TopicPartition> partitions = records.partitions();
            for( TopicPartition partition : partitions) {
                System.out.println("partitionId:" + partition.partition());
                consumer.pause(Arrays.asList(new TopicPartition(topic, partition.partition())));
            }


        }
    }
}
