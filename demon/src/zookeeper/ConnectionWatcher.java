package zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;

public class ConnectionWatcher implements Watcher {
	private static final int SESSION_TIMEOUT = 5000;
	public ZooKeeper zk;
	public CountDownLatch connectedSignal = new CountDownLatch(1);// 需要等待1

	public void connect(String hosts) throws IOException, InterruptedException {
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		connectedSignal.await();
		// 在使用zookeeper对象前，等待连接建立。这里利用Java的CountDownLatch类//（java.util.concurrent.CountDownLatch）来阻塞，直到zookeeper实例准备好。
	}

	public void close() throws InterruptedException {
		zk.close();
	}

	public void create(String groupName) throws KeeperException,
			InterruptedException {
		String path = "/" + groupName;
		String createdPath = zk.create(path, null, Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		System.out.println("Created" + createdPath);
	}

	public void read(String path) throws KeeperException, InterruptedException {
		byte[] data = zk.getData(path, false, new Stat());
		// 为了保存最新一份数据
		String out_putpath = new String(data);
		System.out.println(out_putpath);
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {// 在收到连接事件KeeperState.SyncConnected时，connectedSignal被创建时，计数为1，代表需要在
															// 释放所有等待线程前发生事件的数量。在调用一次countDown()方法后，此计数器会归零，await操作返回。
			connectedSignal.countDown();
		}
	}

	public static void main(String[] args) throws Exception {
		ConnectionWatcher cg = new ConnectionWatcher();
		cg.connect("172.16.0.90:2181");
		// cg.connect("hadoop-2:2181");
		cg.read("/mapreduce/cal_status_outpath_flag");
		// cg.create(args[1]);
		cg.close();
	}

}