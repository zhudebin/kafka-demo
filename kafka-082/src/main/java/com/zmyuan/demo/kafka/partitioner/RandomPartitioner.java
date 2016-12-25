package com.zmyuan.demo.kafka.partitioner;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import java.util.Random;

/**
 * Created by zdb on 2016/10/11.
 */
public class RandomPartitioner implements Partitioner {

    public RandomPartitioner(VerifiableProperties verifiableProperties) {
        super();
        System.out.println("init partitioner RandomPartitioner(VerifiableProperties verifiableProperties)");
    }

    public RandomPartitioner() {
        super();
        System.out.println("init partitioner ()");
    }

    private Random random = new Random();

    public int partition(Object key, int numPartitions) {
        int partitionId = random.nextInt(numPartitions);
        return partitionId;
    }
}
