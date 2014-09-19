package ssdb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;

public class SSDB4jClient {
	public static AtomicLong thread_count ;
	private SSDB ssdb;

	public SSDB getSsdb() {
		return ssdb;
	}

	public void init(String ip, String port,int thread) {
		int p = 8888;
		try {
			p = Integer.parseInt(port);
		} catch (NumberFormatException e) {
		}
		ssdb = SSDBs.pool(ip, p, 2000, null);
		System.out.println("init ...");
		Response resp = ssdb.flushdb("");
		thread_count = new AtomicLong(thread);
		System.out.println("init success");
	}

	public void depose() throws IOException {
		ssdb.close();
		System.out.println("close sccuess");
	}

	public void testScan() {
		for (int i = 0; i < 1000; i++) {
			ssdb.set("key" + i, i);
		}
		Response resp = ssdb.scan("", "", -1);
		Map<String, String> values = resp.mapString();
		resp = ssdb.scan("", "", 900);
		System.out.println(resp.mapString().size());
	}

	public void test_batch() {
		SSDB ssdb = this.ssdb.batch();
		for (int i = 0; i < 1000; i++) {
			ssdb.set("aaa" + i, i);
		}
		System.out.println(System.currentTimeMillis());
		List<Response> resps = ssdb.exec();
		System.out.println(System.currentTimeMillis());
		for (Response resp : resps) {
			System.out.println(resp.ok());
		}
	}

	public void testHset() {
		ssdb.del("my_map");
		ssdb.hset("my_hash", "name", "wendal");
		ssdb.hset("my_hash", "age", 27);

		Response resp = ssdb.hget("my_hash", "name");
		System.out.println(resp.ok());
		System.out.println("wendal:" + resp.asString());
		resp = ssdb.hget("my_hash", "age");
		System.out.println(resp.ok());
		System.out.println(27 == resp.asInt());

		ssdb.hincr("my_hash", "age", 4);
		resp = ssdb.hget("my_hash", "age");
		System.out.println(resp.ok());
		System.out.println(31 == resp.asInt());

		resp = ssdb.hsize("my_hash");
		System.out.println(resp.ok());
		System.out.println(2 == resp.asInt());

		resp = ssdb.hdel("my_hash", "name");
		System.out.println(resp.ok());
		resp = ssdb.hdel("my_hash", "age");
		System.out.println(resp.ok());

		resp = ssdb.hsize("my_hash");
		System.out.println(resp.ok());
		System.out.println(0 == resp.asInt());
	}

	public static void main(String[] args) throws IOException {
		int i = 0;
		String file = "uid";
		String host = "offline";
		String port = "30322";
		int thread_c = 5;
		if (args.length > 3) {
			host = args[0];
			port = args[1];
			file = args[2];
			thread_c = Integer.parseInt(args[3]);
		}
		System.out.println("start test");
		SSDB4jClient client = new SSDB4jClient();
		client.init(host, port,thread_c);
		List<Runner> runners = new ArrayList<SSDB4jClient.Runner>();
		for(long j=thread_c;j>0;j--){
			Runner runner = client.new Runner();
			runners.add(runner);
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] ids = line.split("\\s");
			for (String key : ids) {
				int index =  i++ % thread_c;
				runners.get(index).addKey(key);
			}
		}
		ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
		
		long start = System.currentTimeMillis();
		
		for(Runner runner:runners){
			cachedThreadPool.submit(runner);
		}
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

		// SSDB ssdb;

		// public Runner(SSDB ssdb) {
		// this.ssdb = ssdb;
		// }
		//
		public void addKey(String key) {
			keys.add(key);
		}

		@Override
		public void run() {
			for (String key : keys) {
				testOwn(key);
			}
			thread_count.getAndDecrement();
		}

		public void testOwn(String key) {
			Response resp = ssdb.hscan(key, "", "", 100);
			for (Entry<String, Object> entry : resp.map().entrySet()) {
				System.out.println(entry.getKey() + ":" + new String((byte[]) entry.getValue()));
			}

		}
		public void testOwn_mul(String key) {
			Response resp = ssdb.multi_hget(key, "uid","navy_base_data","agent","every_hour_status_count","avg_forward_status","avg_commemts_status","interact_user","avg_status_num");
			for (Entry<String, Object> entry : resp.map().entrySet()) {
				System.out.println(entry.getKey() + ":" + new String((byte[]) entry.getValue()));
			}
		}
	}

}
