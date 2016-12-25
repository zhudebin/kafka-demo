package com.zmyuan.demo.kafka.election;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * 先到先得的公平模式， 采用各参与方都注册ephemeral_sequential节点，ID较小者为leader
 * 竞选成功则返回true，否则返回失败，但要能支持回调
 * Created by zdb on 2016/11/6.
 */
public class FairElectionCallback {

    private String zkUrl;
    private String parentPath;
    private ZooKeeper zk;
    private String id;

    private ElectionInfo electionInfo;

    public FairElectionCallback(String zkUrl, String parentPath, String id) throws IOException {
        this.zkUrl = zkUrl;
        this.parentPath = parentPath;
        this.id = id;
        init();
    }

    private void init() throws IOException {
        electionInfo = new ElectionInfo(null, null);
        zk = new ZooKeeper(zkUrl, 6000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.toString());
            }
        });
        // 判断根目录是否存在，不存在则创建
        try {
            Stat stat = zk.exists(parentPath, null);
            if( stat == null) {
                zk.create(parentPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean elect(final ElectCallback callback) throws KeeperException, InterruptedException {
        // 判断当前节点是否已经参与竞选
        if(electionInfo.getPath() == null) {
            // 先在父目录下创建临时节点，
            electionInfo.setPath(zk.create(parentPath + "/elect", ("{version=1, id=" + id + "}").getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL));
            List<String> paths = zk.getChildren(parentPath, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("=================子节点触发事件" + event);
                    // 获取最小号，如果最小号和以前leader不同，则需要切换一下leader
                    try {
                        List<String> childPaths = zk.getChildren(parentPath, null);
                        String _leader = getLeader(childPaths);
                        if(_leader != null && _leader.equals(electionInfo.getLeaderPath())) {
                            System.out.println("leader 没有更改, leader:" + _leader);
                        } else {
                            System.out.println("leader changed, leader:" + _leader);
                            // 如果是自己被刪除了，需要复原path为空,方便下一次竞选
                            if(!childPaths.contains(electionInfo.getPath())) {
                                electionInfo.setPath(null);
                            }
                            changeLeaderPath(_leader, callback);
                        }

                        zk.getChildren(parentPath, this);
                        System.out.println("=================子节点触发事件  處理結束" + event);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            initLeader(getLeader(paths), callback);
        }
        if( electionInfo.getPath().equals(electionInfo.getLeaderPath())) {
            callback.callback(1, electionInfo.getLeaderPath());    // 回调
            return true;
        } else {
            callback.callback(2, electionInfo.getLeaderPath());    // 回调
            return false;
        }

    }

    public void autoElect(final ElectCallback callback) throws KeeperException, InterruptedException {
        while(1 == 1) {
            elect(new ElectCallback() {
                @Override
                public void callback(int flag, String leaderPath) throws KeeperException, InterruptedException {
                    switch (flag) {
                        case 1:
                            callback.callback(flag, leaderPath);
                            break;
                        case 2:
                        case 3:
                            callback.callback(flag, leaderPath);
                            break;
                        default:
                            throw new IllegalArgumentException("flag " + flag);
                    }
                }
            });
            Thread.sleep(1000);
        }
    }

    private String getLeader(List<String> paths) {
        if(paths.size() == 0) {
            return null;
        }
        Collections.sort(paths);
        return mkFullPath(paths.get(0));
    }

    private void changeLeaderPath(String path, ElectCallback callback) throws KeeperException, InterruptedException {
        electionInfo.setLeaderPath(mkFullPath(path));
        callback.callback(3, electionInfo.getLeaderPath());
    }

    private void initLeader(String path, ElectCallback callback) throws KeeperException, InterruptedException {
        electionInfo.setLeaderPath(path);
    }

    private String mkFullPath(String path) {
        if(path.startsWith("/")) {
            return path;
        } else {
            return parentPath + "/" + path;
        }
    }

    private class ElectionInfo {
        // 参与竞选的path, 如果为空则还没有参与竞选
        private String path;
        private String leaderPath;

        public ElectionInfo(String path, String leaderPath) {
            this.path = path;
            this.leaderPath = leaderPath;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {

            this.path = path;
        }

        public String getLeaderPath() {
            return leaderPath;
        }

        public void setLeaderPath(String leaderPath) {
            this.leaderPath = leaderPath;
        }
    }

}

interface ElectCallback {

    /**
     *
     * @param flag 1 成功， 2 失败， 3 切换leader
     * @param leaderPath
     * @throws KeeperException
     * @throws InterruptedException
     */
    void callback(int flag, String leaderPath) throws KeeperException, InterruptedException;

}
