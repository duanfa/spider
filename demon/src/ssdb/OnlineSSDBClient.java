package ssdb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.udpwork.ssdb.Response;
import com.udpwork.ssdb.SSDB;

public class OnlineSSDBClient {
	public static AtomicLong thread_count = new AtomicLong(5);;
	SSDB ssdb = null;
	String host ;
	int p;
	public void get(String key) {

		Response res = null;
		try {
			res = ssdb.hscan(key, "", "", 50);
		} catch (Exception e) {
			e.printStackTrace();
		}
		for (Entry<byte[], byte[]> entry : res.items.entrySet()) {
			System.out.println(entry.getKey() + ":" + new String(entry.getValue()));
		}
	}

	private void init(String host, String port) {
		try {
			this.host = host;
			this.p = Integer.parseInt(port);
			ssdb = new SSDB(host,p , 10000);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void depose() {
		ssdb.close();
	}

	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		int i = 0;
		String file = "uid";
		String host = "offline";
		String port = "30322";
		if (args.length > 2) {
			host = args[0];
			port = args[1];
			file = args[2];
		}
		System.out.println("start test");
		OnlineSSDBClient client = new OnlineSSDBClient();
		client.init(host, port);
		Runner runner1 = client.new Runner();
		Runner runner2 = client.new Runner();
		Runner runner3 = client.new Runner();
		Runner runner4 = client.new Runner();
		Runner runner5 = client.new Runner();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] ids = line.split("\\s");
			for (String key : ids) {
				switch (i++ % 5) {
				case 0:
					runner1.addKey(key);
					break;
				case 1:
					runner2.addKey(key);
					break;
				case 2:
					runner3.addKey(key);
					break;
				case 3:
					runner4.addKey(key);
					break;
				case 4:
					runner5.addKey(key);
					break;
				}
			}
		}
		ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
		cachedThreadPool.submit(runner1);
		cachedThreadPool.submit(runner2);
		cachedThreadPool.submit(runner3);
		cachedThreadPool.submit(runner4);
		cachedThreadPool.submit(runner5);
		cachedThreadPool.shutdown();
		while (true) {
			if (thread_count.get() == 0) {
				System.out.println(i + " keys use:" + (System.currentTimeMillis() - start));
				br.close();
				client.depose();
				break;
			}
		}
	}

	class Runner implements Runnable {

		List<String> keys = new ArrayList<String>();
		SSDB ssdbo;
		
		public void addKey(String key) {
			keys.add(key);
		}

		@Override
		public void run() {
			try {
				ssdbo = new SSDB(host, p, 10000);
			} catch (Exception e) {
				e.printStackTrace();
			}
			for (String key : keys) {
				testOwn(key);
			}
			thread_count.getAndDecrement();
		}

		public void testOwn(String key) {
			Response res = null;
			try {
				res = ssdbo.hscan(key, "", "", 50);
			} catch (Exception e) {
				System.out.println("key:"+key);
				e.printStackTrace();
			}
			for (Entry<byte[], byte[]> entry : res.items.entrySet()) {
				System.out.println(new String(entry.getKey() )+ ":" + new String(entry.getValue()));
			}
		}
	}

}
