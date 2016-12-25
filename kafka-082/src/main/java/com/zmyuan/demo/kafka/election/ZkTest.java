package com.zmyuan.demo.kafka.election;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * Created by zdb on 2016/11/6.
 */
public class ZkTest {

    public static void main(String[] args) throws Exception {

        ZooKeeper zk = new ZooKeeper("192.168.0.150", 6000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.toString());
            }
        });
        List<String> paths = zk.getChildren("/", null);

        for(String path : paths) {
            System.out.println(path);
        }

        Stat stat = zk.exists("/testhello", null);
        System.out.println(stat);
    }
}
