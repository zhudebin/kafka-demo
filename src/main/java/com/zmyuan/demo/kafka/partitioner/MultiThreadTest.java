package com.zmyuan.demo.kafka.partitioner;

/**
 * Created by zdb on 2016/10/11.
 */
public class MultiThreadTest extends Thread {

    @Override
    public void run() {
        PartitionerProducter pp = new PartitionerProducter();

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            pp.sendOne("test1");
        }
    }

    public static void main(String[] args) throws InterruptedException {

        for(int i=0; i<10; i++) {
            new MultiThreadTest().start();
        }

        while(1 == 1) {
            Thread.sleep(100000);
        }
    }

}
