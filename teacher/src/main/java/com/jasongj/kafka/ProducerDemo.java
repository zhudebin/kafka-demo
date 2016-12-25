package com.jasongj.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class ProducerDemo {

  static private final String TOPIC = "topic0";
  static private final String BROKER_LIST = "n150:9092";


  public static void main(String[] args) throws Exception {
    Producer<String, String> producer = initProducer();
    while (1==1) {
      sendOne(producer, TOPIC);
    }
  }

  private static Producer<String, String> initProducer() {
    Properties props = new Properties();
    props.put("metadata.broker.list", BROKER_LIST);
    // props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("serializer.class", StringEncoder.class.getName());
    props.put("partitioner.class", HashPartitioner.class.getName());
//    props.put("compression.codec", "0");
    props.put("producer.type", "sync");
    props.put("batch.num.messages", "1");
    props.put("queue.buffering.max.messages", "1000000");
    props.put("queue.enqueue.timeout.ms", "20000000");

    ProducerConfig config = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<String, String>(config);
    return producer;
  }

  public static void sendOne(Producer<String, String> producer, String topic) throws InterruptedException {
	boolean sleepFlag = false;
    KeyedMessage<String, String> message1 = new KeyedMessage<String, String>(topic, "1", "test 0");
    producer.send(message1);
    if(sleepFlag) Thread.sleep(5000);
    KeyedMessage<String, String> message2 = new KeyedMessage<String, String>(topic, "2", "test 1");
    producer.send(message2);
    if(sleepFlag) Thread.sleep(5000);
    KeyedMessage<String, String> message3 = new KeyedMessage<String, String>(topic, "3", "test 2");
    producer.send(message3);
//    producer.close();
  }

}
