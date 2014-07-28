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
	public CountDownLatch connectedSignal = new CountDownLatch(1);// ��Ҫ�ȴ�1

	public void connect(String hosts) throws IOException, InterruptedException {
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		connectedSignal.await();
		// ��ʹ��zookeeper����ǰ���ȴ����ӽ�������������Java��CountDownLatch��//��java.util.concurrent.CountDownLatch����������ֱ��zookeeperʵ��׼���á�
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
		// Ϊ�˱�������һ������
		String out_putpath = new String(data);
		System.out.println(out_putpath);
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {// ���յ������¼�KeeperState.SyncConnectedʱ��connectedSignal������ʱ������Ϊ1��������Ҫ��
															// �ͷ����еȴ��߳�ǰ�����¼����������ڵ���һ��countDown()�����󣬴˼���������㣬await�������ء�
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