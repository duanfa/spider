package leveldb;import java.io.BufferedReader;import java.io.ByteArrayOutputStream;import java.io.DataOutputStream;import java.io.File;import java.io.IOException;import java.io.InputStreamReader;import java.net.URI;import java.util.concurrent.ExecutorService;import java.util.concurrent.Executors;import java.util.concurrent.atomic.AtomicLong;import net.sf.json.JSONObject;import org.apache.commons.lang.StringUtils;import org.apache.hadoop.conf.Configuration;import org.apache.hadoop.fs.FSDataInputStream;import org.apache.hadoop.fs.FileStatus;import org.apache.hadoop.fs.FileSystem;import org.apache.hadoop.fs.Path;import org.apache.hadoop.hdfs.DistributedFileSystem;import org.apache.zookeeper.WatchedEvent;import org.apache.zookeeper.Watcher;import org.apache.zookeeper.ZooKeeper;import org.apache.zookeeper.data.Stat;import org.fusesource.leveldbjni.JniDBFactory;import org.iq80.leveldb.DB;import org.iq80.leveldb.DBFactory;import org.iq80.leveldb.Options;import org.iq80.leveldb.WriteBatch;public class BunchImportLevelDB {	public static AtomicLong increace_thread_complate_flag = new AtomicLong(0);	public static long start = 0l;	public static void main(String[] args) {		System.out.println("程序开始运行.........");		// 写入levelDB 数据文件路径		String leveldb_data_path = args[0];		// 数据源文件目录		String zookeeperhost_ip = args[1];		// String org_data_path = getInputPath(zookeeperhost_ip);		String org_data_path = args[2];		Configuration conf = new Configuration();		// conf.addResource("/usr/local/hadoop-2.0.0-cdh4.0.0/etc/hadoop/core-site.xml");		// conf.addResource("/usr/local/hadoop-2.0.0-cdh4.0.0/etc/hadoop/hdfs-site.xml");		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());		String dst = "hdfs://hadoop-1:9000/";		FileStatus[] stats = null;		int file_size = 1;		try {			FileSystem hdfs = FileSystem.get(URI.create(dst), conf);			Path listf = new Path(org_data_path);			stats = hdfs.listStatus(listf);			file_size = stats.length;			System.out.println("hadoop 问价系统加载成功");		} catch (IOException e2) {			System.out.println("hadoop 文件系统加载失败");			e2.printStackTrace();			return;		}		// 最大线程数		Integer run_threads = Integer.valueOf(args[3]);		// 解析数据式，0：文本格式 1，json格式		String parseDataType = args[4];		// 解析的数据属性		String parseDataAttr = args[5];		// 每天平均发微博最大值		double avg_status_num_max = getMaxAvgNum(zookeeperhost_ip);		ExecutorService executorService = Executors.newFixedThreadPool(run_threads);		if (run_threads > file_size) {			System.out.println("run_threads:" + run_threads + "最大线程数必须小于或等于：" + org_data_path + "文件夹下文件数量:" + file_size);			return;		}		int total_thread_complate_flag = file_size;		DBFactory factory = JniDBFactory.factory;		Options options = new Options().createIfMissing(true);		options.writeBufferSize(25000000);		DB db = null;		try {			db = factory.open(new File(leveldb_data_path), options);		} catch (IOException e1) {			System.out.println(leveldb_data_path + "数据文件打开失败:" + e1);			e1.printStackTrace();		}		start = System.currentTimeMillis();		System.out.println("数据开始写入 start time:" + start);		for (int i = 0; i < file_size; i++) {			// new Thread(new LevelThread(db,childfile[i])).start();			executorService.execute(new LevelThread(db, stats[i].getPath().toUri(), parseDataType, parseDataAttr, avg_status_num_max, conf));		}		while (total_thread_complate_flag != increace_thread_complate_flag.get()) {			try {				Thread.sleep(60000);			} catch (InterruptedException e) {				e.printStackTrace();			}			System.out.println("数据正在导入，请稍后===============程序已经运行" + (System.currentTimeMillis() - start) / 1000 + "秒");		}		System.out.println("数据导入完毕===============共运行" + (System.currentTimeMillis() - start) / 1000 + "秒");		try {			if (db != null) {				db.close();			}			executorService.shutdown();		} catch (IOException e) {			System.out.println(leveldb_data_path + "数据库关闭失败");			e.printStackTrace();		}	}	public static String getInputPath(String zookeeperhost_ip) {		try {			ZooKeeper zk = new ZooKeeper(zookeeperhost_ip, 20 * 1000, new Watcher() {				@Override				public void process(WatchedEvent event) {				}			});			byte[] data = zk.getData("/mapreduce/cal_status_outpath_flag", false, new Stat());			String path = new String(data);			return path.split("--")[1];		} catch (Exception e) {			System.out.println("getInputPath(String zookeeperhost_ip) + 错误" + e);			e.printStackTrace();		}		return null;	}	public static double getMaxAvgNum(String zookeeperhost_ip) {		try {			ZooKeeper zk = new ZooKeeper(zookeeperhost_ip, 20 * 1000, new Watcher() {				@Override				public void process(WatchedEvent event) {				}			});			byte[] data = zk.getData("/mapreduce/avg_num_max", false, new Stat());			return Double.valueOf(new String(data));		} catch (Exception e) {			System.out.println("getMaxAvgNum(String zookeeperhost_ip) + 错误" + e);			e.printStackTrace();		}		return 1.0;	}}class LevelThread implements Runnable {	private DB db;	private URI org_data_path;	private String parseDataType;	private String parseDataAttr;	private Double avg_status_num_max;	private ByteArrayOutputStream bos = new ByteArrayOutputStream(512);	private DataOutputStream dos = new DataOutputStream(bos);	private Configuration conf;	private WriteBatch batch;	public LevelThread(DB db, URI org_data_path, String parseDataType, String parseDataAttr, Double avg_status_num_max, Configuration conf) {		this.db = db;		this.org_data_path = org_data_path;		this.parseDataType = parseDataType;		this.parseDataAttr = parseDataAttr;		this.avg_status_num_max = avg_status_num_max;		this.conf = conf;		batch = this.db.createWriteBatch();	}	@Override	public void run() {		try {			FileSystem fs = FileSystem.get(org_data_path, conf);			FSDataInputStream in = fs.open(new Path(org_data_path), 5 * 1024 * 1024);			BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"), 5 * 1024 * 1024);// 用5M的缓冲读取文本文件			String line = null;			String[] attrs = new String[] {};			;			// 1:json格式			if ("1".equals(parseDataType)) {				attrs = StringUtils.splitByWholeSeparator(parseDataAttr, ",");			}			long count = 0l;			while ((line = reader.readLine()) != null) {				// 1:json格式				if ("1".equals(parseDataType)) {					JsonToLevelDB(attrs, line);					if (count % 10000 == 0) {						db.write(batch);					}				}				if ("0".equals(parseDataType)) {					TextToLevelDB(parseDataAttr, line);					if (count % 10000 == 0) {						db.write(batch);					}				}			}			db.write(batch);			// System.out.println(org_data_path.getPath()			// +"=========处理结束, 用时："+(System.currentTimeMillis()-ImportLevelDB.start)/1000+"秒");			System.out.println(org_data_path.getPath() + "=========处理结束");			batch.close();			dos.close();			bos.close();			reader.close();		} catch (Exception e) {			System.out.println("运行" + org_data_path.getPath() + "失败" + e);			e.printStackTrace();		} finally {			BunchImportLevelDB.increace_thread_complate_flag.incrementAndGet();		}	}	public void JsonToLevelDB(String[] attrs, String json) {		JSONObject jsonObject = JSONObject.fromObject(json);		String uid = jsonObject.getString(attrs[0]);		// 解析出字段写入 leveldb		for (int i = 1; i < attrs.length; i++) {			// 水度			if ("navy_base_data".equals(attrs[i])) {				String shuidubase = jsonObject.getString("navy_base_data");				/*				 * String[] navys =				 * StringUtils.splitByWholeSeparator(shuidubase, "-"); double				 * zhunafalav = Double.valueOf(navys[3]); double zhuanfa =				 * Double.valueOf(navys[1]); double dnavy = 0; if(zhunafalav > 0				 * && zhuanfa > 0){ dnavy =				 * Double.valueOf(navys[3])/Double.valueOf(navys[1]); dnavy =				 * Math.round(dnavy * 100000)/ 100000d; }				 */				String levelkey = encodeHashCode(uid, "shuidu");				batch.put(org.fusesource.leveldbjni.JniDBFactory.bytes(levelkey), org.fusesource.leveldbjni.JniDBFactory.bytes(shuidubase));				// 活跃度			} else if ("avg_status_num".equals(attrs[i])) {				double avg_status_num = jsonObject.getDouble("avg_status_num");				double rate = avg_status_num / avg_status_num_max;				double liveness = Math.log((double) (rate + 1)) / Math.log(2);				liveness = Math.round(liveness * 100000) / 100000d;				if (liveness > 1) {					liveness = Double.valueOf(String.valueOf(liveness).replaceAll("\\d+\\.[0-9]{1}", "0.9"));				}				String levelkey = encodeHashCode(uid, "liveness");				batch.put(org.fusesource.leveldbjni.JniDBFactory.bytes(levelkey), org.fusesource.leveldbjni.JniDBFactory.bytes(String.valueOf(liveness)));			} else {				String value = jsonObject.getString(attrs[i]);				String levelkey = encodeHashCode(uid, attrs[i]);				batch.put(org.fusesource.leveldbjni.JniDBFactory.bytes(levelkey), org.fusesource.leveldbjni.JniDBFactory.bytes(value));			}		}	}	public void TextToLevelDB(String attr, String line) {		try {			String[] values = new String[] {};			if (line.contains(" ")) {				String[] attrs = StringUtils.splitByWholeSeparator(attr, ",");				// 真假uid				values = StringUtils.splitByWholeSeparator(line, " ");				String value = values[1];				String levelkey = encodeHashCode(values[0], attrs[1]);				Double real_degree = Math.round(Double.valueOf(value) * 100000) / 100000d;				batch.put(org.fusesource.leveldbjni.JniDBFactory.bytes(levelkey), org.fusesource.leveldbjni.JniDBFactory.bytes(String.valueOf(real_degree)));			}			// pr处理			if (line.contains("\t")) {				values = StringUtils.splitByWholeSeparator(line, "\t");				String[] attrs = StringUtils.splitByWholeSeparator(attr, ",");				// 未经处理pr				String value = values[1];				Double pr = Math.round(Double.valueOf(value) * 100000000) / 100000000d;				String levelkey = encodeHashCode(values[0], attrs[1]);				batch.put(org.fusesource.leveldbjni.JniDBFactory.bytes(levelkey), org.fusesource.leveldbjni.JniDBFactory.bytes(String.valueOf(pr)));				Double dispose_pr = Double.valueOf(value) / 377145.0625d;				dispose_pr = Math.log(dispose_pr + 1) / Math.log(2);				dispose_pr = Math.round(dispose_pr * 100000000) / 100000000d;				levelkey = encodeHashCode(values[0], attrs[2]);				batch.put(org.fusesource.leveldbjni.JniDBFactory.bytes(levelkey), org.fusesource.leveldbjni.JniDBFactory.bytes(String.valueOf(dispose_pr)));			}		} catch (Exception e) {			System.out.println("数据=====" + line + "===出现异常");		}	}	public String encodeHashCode(String hashname, String key) {		String result = "";		try {			dos.writeBytes("h");			dos.write(hashname.getBytes().length);			dos.writeBytes(hashname);			dos.writeBytes("=");			dos.writeBytes(key);			result = bos.toString();			bos.reset();		} catch (IOException e) {			System.out.println("hash key 编译失败......hashname:" + hashname + "key:" + key);			e.printStackTrace();		}		return result;	}}