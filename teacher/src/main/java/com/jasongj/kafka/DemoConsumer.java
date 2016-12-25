package com.jasongj.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class DemoConsumer {

	public static void main(String[] args) {
		String zk = "n150:2181";
		String topic = "topic0";
		String groupid = "group1";
		String consumerid = "consumer1";
		Properties props = new Properties();
		props.put("zookeeper.connect", zk);
		props.put("group.id", groupid);
		props.put("client.id", "test");
		props.put("consumer.id", consumerid);
		props.put("auto.offset.reset", "smallest");
		props.put("auto.commit.enable", "true");
		props.put("auto.commit.interval.ms", "100");

        // 将offset 存储到kafka
//        props.put("offsets.storage", "kafka");
        // 将offset 写入kafka和zk 两份
//        props.put("dual.commit", "true");

		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);

		KafkaStream<byte[], byte[]> stream1 = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> interator = stream1.iterator();
		while (interator.hasNext()) {
			MessageAndMetadata<byte[], byte[]> messageAndMetadata = interator.next();
			String message = String.format(
					"Topic:%s, GroupID:%s, Consumer ID:%s, PartitionID:%s, Offset:%s, Message Key:%s, Message Payload: %s",
					messageAndMetadata.topic(), groupid, consumerid, messageAndMetadata.partition(),
					messageAndMetadata.offset(), new String(messageAndMetadata.key()),
					new String(messageAndMetadata.message()));
			System.out.println(message);
		}
	}

}
