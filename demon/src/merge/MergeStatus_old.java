package merge;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;

import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.st.util.ConfigurationUtils;
import com.st.util.HdfsUtils;
import com.st.util.ZKUtils;

/**
 *  输入路径：
 *  clearOriginalStatus
	clearForwardStatus
	/scribedata/climb2.0/status_forward
	/scribedata/climb2.0/original_status
	
	通过sid进行去重
 * 根据sid对以上数据进行去重合并
 */
public class MergeStatus_old extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(MergeStatus_old.class);

	/***************** map ********************/
	public static class Map extends Mapper<Object, Text, Text, Text> {

		private static Text key = new Text();
		private static Text value = new Text();

		public void map(Object keyIn, Text valueIn, Context context) {
			if (null == valueIn || valueIn.getLength() <= 0) {
				return;
			}
			try {
				String json = valueIn.toString();
				JSONObject jsonObject = JSONObject.fromObject(json);
				if(jsonObject.containsKey("sid") ){
					String sid = jsonObject.getString("sid");
					System.out.println(sid+InetAddress.getLocalHost().getHostName());
					key.set(sid);
					value.set(valueIn);
					context.write(key,value);
				}
			} catch (Exception e) {
				logger.error(" Mapper.map. ERROR : uid=" + key + "or org_uid = "+valueIn );
			} 
		}
	}

	/***************** reduce ********************/
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) {
			
			for (Iterator<Text>  iterator = values.iterator(); iterator.hasNext();) {
				Text status =  iterator.next();
				try {
					context.write(status, new Text(""));
					break;
				} catch (Exception e) {
					logger.error(" reducer.reduce. ERROR : sid=" + key );
				}
			}
		}
	}

	/***************** main ********************/

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		// class name.
		String jobName = MergeStatus_old.class.getName();
		String shortName = jobName.substring(jobName.lastIndexOf(".") + 1);

		String inputPath = args[0];
		String zookeeperConnection = args[1];
		
		String[] inputPaths = inputPath.split(",");
		
		String outputFilePath = null;
		
		
		
		ConfigurationUtils.setThirdJars(conf);
//		conf.set("mapreduce.map.java.opts", "-Xmx3072M");
//		conf.set("mapreduce.map.memory.mb", "4096");
		
//		conf.set("mapreduce.reduce.java.opts", "-Xmx3072M");
//		conf.set("mapreduce.reduce.memory.mb", "4096");
		
		Job job = Job.getInstance(conf, shortName);
		
		job.setJarByClass(MergeStatus_old.class);

		// set mapper
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// set reducer
		job.setReducerClass(Reduce.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// set input-path
		
		for (int i = 0; i < inputPaths.length; i++) {
			String pathString = inputPaths[i];
			FileInputFormat.addInputPath(job, new Path(pathString));
		}
		
		/*****************************以下为输入路径处理开始*********************************************/
		createZNode(zookeeperConnection);
		//上一次执行完，写进zk的数据
		String outputFilePathFlag = getZnodeData(zookeeperConnection);
		String[] outputFilePathFlags = outputFilePathFlag.split("--");
		if("0".equals(outputFilePathFlags[0])){
			
			outputFilePath = outputFilePathFlags[1];
			
			//上一次输入路径作为本次输入
			StringBuffer sb = new StringBuffer(outputFilePath);
			//本次要输出路径
			outputFilePath = sb.substring(0, sb.length()-1)+"1";
			
		}
		
		if("1".equals(outputFilePathFlags[0])){
			
			outputFilePath = outputFilePathFlags[1];
			//上一次输入路径作为本次输入
			StringBuffer sb = new StringBuffer(outputFilePath);
			//本次要输出路径
			outputFilePath = sb.substring(0, sb.length()-1)+"0";
			
		}
		
		//删除将要执行的那个路径
		HdfsUtils.deleteFile(conf, outputFilePath);
		FileOutputFormat.setOutputPath(job, new Path(outputFilePath));
		
		
		/*****************************以下为输出路径处理结束*********************************************/
		if(job.waitForCompletion(true)){
			String pre_PutputFilePath = null;
			if("0".equals(outputFilePathFlags[0])){
				pre_PutputFilePath = outputFilePathFlags[1];
				StringBuffer sb = new StringBuffer(pre_PutputFilePath);
				setZnodeData(zookeeperConnection, "/mapreduce/merge_status_outpath_flag", ("1--"+sb.substring(0, sb.length()-1)+"1").getBytes());
			}
			if("1".equals(outputFilePathFlags[0])){
				pre_PutputFilePath = outputFilePathFlags[1];
				StringBuffer sb = new StringBuffer(pre_PutputFilePath);
				setZnodeData(zookeeperConnection, "/mapreduce/merge_status_outpath_flag", ("0--"+sb.substring(0, sb.length()-1)+"0").getBytes());
			}
			//为保留最新数据，删除前一版本数据
			HdfsUtils.deleteFile(conf, pre_PutputFilePath);
			return 0;
		}else{
			return -1;
		}
	}
	
	
	/**
	 * 在相应节点中写入 mapreduce输出路径
	 */
	public static void createZNode(String zookeeperConnection) throws Exception{
		ZooKeeper zooKeeper = ZKUtils.getConnection(zookeeperConnection, 30*1000, new Watcher(){
			@Override
			public void process(WatchedEvent event) {
			}
		});
		
		Stat stat = zooKeeper.exists("/mapreduce", false);
		if(stat == null){
			zooKeeper.create("/mapreduce", "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		stat = zooKeeper.exists("/mapreduce/merge_status_outpath_flag", false);
		if(stat == null){
			zooKeeper.create("/mapreduce/merge_status_outpath_flag", "0--/data/status/merge_status_0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		zooKeeper.close();
	}
	
	public static String getZnodeData(String zookeeperConnection) throws Exception{
		String out_putpath = "0";
		
		ZooKeeper zooKeeper = ZKUtils.getConnection(zookeeperConnection, 30*1000, new Watcher(){
			@Override
			public void process(WatchedEvent event) {
			}
		});
		
		byte[] data = zooKeeper.getData("/mapreduce/merge_status_outpath_flag", false, new Stat());
		//为了保存最新一份数据
		out_putpath = new String(data);
		/**/
		zooKeeper.close();
		return out_putpath;
	}
	
	public static void setZnodeData(String zookeeperConnection,String path,byte[] data) throws Exception{
		
		
		ZooKeeper zooKeeper = ZKUtils.getConnection(zookeeperConnection, 30*1000, new Watcher(){
			@Override
			public void process(WatchedEvent event) {
			}
		});
		
		zooKeeper.setData(path, data, -1);
		zooKeeper.close();
	}

	public static int mergeStatus(String[] args) {
		// run job
		int code = 0;
		try {
			code = ToolRunner.run(new Configuration(),new MergeStatus_old(), args);
		} catch (Exception e) {
			logger.error(" error. ", e);
		}
		logger.info(" main [ end = " + System.currentTimeMillis() + " ]. code=" + code);
		return code;
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		mergeStatus(args);
//		String json = "{\"uid\":\"1786368645\",\"sid\":\"62852\",\"org_uid\":null,\"org_sid\":\"62834\",\"org_user_type\":\"4\",\"created_at\":\"1266824220\",\"user_agent\":\"天翼社区\",\"comments_count\":\"0\",\"retweeted_count\":\"0\"}";
//		JSONObject jsonObject = JSONObject.fromObject(json);
//		String uid = jsonObject.getString("uid");
//		String org_uid = jsonObject.getString("org_uid");
//		if("null".equals(org_uid)){
//			System.out.println(org_uid);
//		}
//		System.out.println(uid+"=="+org_uid);
//		StringBuilder sb = new StringBuilder();
//		sb.append("sss");
//		System.out.println(sb.substring(0, sb.length() - 1));
	}
}































