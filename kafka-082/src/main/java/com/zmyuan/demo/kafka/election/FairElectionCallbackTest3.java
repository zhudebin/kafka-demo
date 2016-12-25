package com.zmyuan.demo.kafka.election;

import org.apache.zookeeper.KeeperException;

/**
 * Created by zdb on 2016/11/6.
 */
public class FairElectionCallbackTest3 {

    public static void main(String[] args) throws Exception {

        final FairElectionCallback election = new FairElectionCallback("192.168.0.150:2181", "/test-elect", "id_003");
        election.autoElect(new ElectCallback() {
            @Override
            public void callback(int flag, String leaderPath) throws KeeperException, InterruptedException {

//                System.out.println("call back success " + flag + "-" + leaderPath);

                switch (flag) {
                    case 1:
                        System.out.println("竞选成功，leader:" + leaderPath);
                        break;
                    case 2:
//                        System.out.println("竞选失败, leader : " + leaderPath);
                        System.out.print(".");
                        break;
                    case 3:

                        System.out.println("leader 切换, leader:" + leaderPath);
                        break;
                    default:
                        System.out.println("暫無次選項");
                }

//                election.elect(this);
            }
        });
        while (1 == 1) {
            Thread.sleep(500);
        }
    }
}
