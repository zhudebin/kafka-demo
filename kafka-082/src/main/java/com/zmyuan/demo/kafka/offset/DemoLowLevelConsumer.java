package com.zmyuan.demo.kafka.offset;

import kafka.api.FetchRequestBuilder;
import kafka.api.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;

/**
 * Created by zdb on 2016/11/19.
 */
public class DemoLowLevelConsumer {


    public static void main(String[] args) {

        final String topic = "topic0";

        String clientId = "demoLowLevel1";
        SimpleConsumer simpleConsumer = new SimpleConsumer("n150", 9092, 100000, 65 * 1000000, clientId);

        FetchRequest request = new FetchRequestBuilder()
                .clientId(clientId)
                .addFetch(topic, 0, 0L, 1000000)
                .addFetch(topic, 1, 2L, 5000)
                .addFetch(topic, 2, 0L, 1000000)
                .build();

        FetchResponse response = simpleConsumer.fetch(request);
        for(int i=0; i<3; i++) {
            ByteBufferMessageSet messageSet = response.messageSet(topic, i);
            for (MessageAndOffset messageAndOffset : messageSet) {
                ByteBuffer payload = messageAndOffset.message().payload();
                long offset = messageAndOffset.offset();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                System.out.println("partion: " + i + ",Offset:" + offset + ",Payload:" + new String(bytes, Charset.forName("utf-8")));
            }
        }
    }


}
