package com.zmyuan.demo.kafka.partitioner;

import kafka.producer.Partitioner;

/**
 * Created by zdb on 2016/10/11.
 */
public class HashPartitioner implements Partitioner {

    public int partition(Object key, int numPartitions) {
        if(key instanceof Integer) {
            return (Integer) key  % numPartitions;
        }
        return key.hashCode() % numPartitions;
    }
}
