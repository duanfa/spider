package mergeLatest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.sf.json.JSONObject;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.st.util.ConfigurationUtils;
import com.st.util.HdfsUtils;
import com.st.util.ZKUtils;
/**
 *   
 * hadoop jar MergeUserLastestStatus.jar com.st.mapreduce2.exp.MergeUserLastestStatus 原始输入路径1,原始输入路径2....    增量抓取路径1,增量抓取路径2.....    输出路径   reduce个数    用户的微博个数
 */
public class MergeUserLastestStatus_old extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(MergeUserLastestStatus_old.class);

	/***************** map ********************/
	public static class Map extends Mapper<Object, Text, UidAndStatusTime, Text> {

		
		private static Text value = new Text();
		private static SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy",java.util.Locale.ENGLISH);
		public void map(Object keyIn, Text valueIn, Context context) {
			
			if (null == valueIn || valueIn.getLength() <= 0) {
				return;
			}
			try {
				String json = valueIn.toString();
				
				JSONObject jsonObject = JSONObject.fromObject(json);
				if(jsonObject.containsKey("uid")&& jsonObject.containsKey("created_at") && jsonObject.containsKey("sid")){
					long uid = jsonObject.getLong("uid");
					String createAt = jsonObject.getString("created_at");
					
					Date date = format.parse(createAt);
					UidAndStatusTime key = new UidAndStatusTime(uid, date.getTime());
					value.set(valueIn);
					context.write(key,value);
				}
			} catch (Exception e) {
				
			} 
		}
	}

	/***************** reduce ********************/
	public static class Reduce extends Reducer<UidAndStatusTime, Text, Text, Text> {
		int lastestStatusSize = 0;
		private static Text keyout = new Text();
		private static Text valout = new Text();
		private static Set<Long> sids = new HashSet<Long>();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			lastestStatusSize = context.getConfiguration().getInt("lastestStatus", 800);
			super.setup(context);
		}

		@Override
		public void reduce(UidAndStatusTime key, Iterable<Text> values, Context context) {
			
			sids.clear();
			int count = 0;
			for (Iterator<Text>  iterator = values.iterator(); iterator.hasNext();) {
				try {
					if(count >= lastestStatusSize){break;}
					String json =  iterator.next().toString();
					JSONObject jsonObject = JSONObject.fromObject(json);
					Long sid = jsonObject.getLong("sid");
					
					if(!sids.contains(sid)){
						sids.add(sid);
						count = count+1;
						keyout.set(json);
						valout.set(""); 
						context.write(keyout,valout);
						
					}else{
						continue;
					}
				} catch (Exception e) {
					logger.error(" reducer.reduce. ERROR : uid=" + key );
				}
			}
		}
	}

	
	/***************** main ********************/

	@Override
	public int run(String[] args) throws Exception {
		
		String orgFilePath = null;
		String outputFilePath = null;
		
		int userStatusSize = 800;
		String zookeeperConnection = null;
		if(args.length != 3){
			logger.error("agrs[] is error ..... agrs is must three"); 
			return -1;
		}
		
		/*if(args.length == 2){
			zookeeperConnection = args[0];
			userStatusSize = Integer.valueOf(args[1]);
		}*/
		
		if(args.length == 3){
			orgFilePath = args[0];
			zookeeperConnection = args[1];
			userStatusSize = Integer.valueOf(args[2]);
		}
		
		// class name.
		String jobName = MergeUserLastestStatus_old.class.getName();
		String shortName = jobName.substring(jobName.lastIndexOf(".") + 1);

		Configuration conf = getConf();
		conf.setInt("lastestStatus", userStatusSize); 
		
		ConfigurationUtils.setThirdJars(conf);

//		conf.set("mapred.max.split.size", "134217728");
//		
//		
//		conf.set("mapred.min.split.size", "268435456");
//		conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.9");
//		
//		conf.set("mapreduce.reduce.java.opts", "-Xmx3072M");
//		conf.set("mapreduce.reduce.memory.mb", "4096");
		
		Job job = Job.getInstance(conf, shortName);
		
		job.setJarByClass(MergeUserLastestStatus_old.class);

		// set mapper
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(UidAndStatusTime.class);
		job.setMapOutputValueClass(Text.class);
		job.setSortComparatorClass(OrgUidAndUidComparator.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setPartitionerClass(PartitionByOrgUid.class);
		
		// set reducer
		job.setReducerClass(Reduce.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		/***************************************以下为输入路径处理***********************************/
		List<String> list = new ArrayList<String>();
		for (int i = 1; i <= 1; i++) {
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.DAY_OF_MONTH, -i); 
			Date date = calendar.getTime();
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			String datefile = format.format(date);
			list.add(datefile);
		}
		
		
		
//		String datefile = "2014-04-25_00117";
		
		

		ZooKeeper zooKeeper = ZKUtils.getConnection(zookeeperConnection, 30*1000, new Watcher(){
			@Override
			public void process(WatchedEvent event) {
			}
		});
		/**********************************数据的输入**************************************/
		String inputpath = new String(zooKeeper.getData("/mapreduce/merge_status_outpath_flag", false, new Stat()));
		zooKeeper.close();
		
		String[] inputpaths = inputpath.split("--");
		//   /data/status/merge_user_lastest_status_0
		
		if(inputpaths.length == 1){
			inputpath = inputpaths[0];
		}else{
			inputpath = inputpaths[1];
		}
		
		//0: 不读取合并后所有微博
		//1：读取合并后所有微博
		if(orgFilePath.equals("1")){
 			FileInputFormat.addInputPath(job, new Path(inputpath));
		}
		
		//增量文件
		if(orgFilePath.equals("0")){
			for (int i = 0; i < list.size(); i++) {
				FileInputFormat.addInputPath(job, new Path("/scribedata/climb2.0/original_status/"+"*"+list.get(i)+"*"));
				FileInputFormat.addInputPath(job, new Path("/scribedata/climb2.0/status_forward/"+"*"+list.get(i)+"*"));
			}
			
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
			
			if(HdfsUtils.existFile(conf, sb.substring(0, sb.length()-1)+"0") && orgFilePath.equals("0")){
				FileInputFormat.addInputPath(job, new Path(sb.substring(0, sb.length()-1)+"0"));
			}
		}
		
		if("1".equals(outputFilePathFlags[0])){
			
			outputFilePath = outputFilePathFlags[1];
			//上一次输入路径作为本次输入
			StringBuffer sb = new StringBuffer(outputFilePath);
			//本次要输出路径
			outputFilePath = sb.substring(0, sb.length()-1)+"0";
			
			if(HdfsUtils.existFile(conf, sb.substring(0, sb.length()-1)+"1") && orgFilePath.equals("0")){
				FileInputFormat.addInputPath(job, new Path(sb.substring(0, sb.length()-1)+"1"));
			}
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
				setZnodeData(zookeeperConnection, "/mapreduce/merge_user_lastest_status_outpath_flag", ("1--"+sb.substring(0, sb.length()-1)+"1").getBytes());
			}
			if("1".equals(outputFilePathFlags[0])){
				pre_PutputFilePath = outputFilePathFlags[1];
				StringBuffer sb = new StringBuffer(pre_PutputFilePath);
				setZnodeData(zookeeperConnection, "/mapreduce/merge_user_lastest_status_outpath_flag", ("0--"+sb.substring(0, sb.length()-1)+"0").getBytes());
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
		
		stat = zooKeeper.exists("/mapreduce/merge_user_lastest_status_outpath_flag", false);
		if(stat == null){
			zooKeeper.create("/mapreduce/merge_user_lastest_status_outpath_flag", "0--/data/status/merge_user_lastest_status_0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
		
		byte[] data = zooKeeper.getData("/mapreduce/merge_user_lastest_status_outpath_flag", false, new Stat());
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
	
	public static int UserStatusLimitSize(String[] args) {
		// run job
		int code = 0;
		try {
			code = ToolRunner.run(new Configuration(),new  MergeUserLastestStatus_old(), args);
			
		} catch (Exception e) {
			logger.error(" error. ", e);
		}
		logger.info(" main [ end = " + System.currentTimeMillis() + " ]. code=" + code);
		return code;
	}
	
	public static class UidAndStatusTime implements WritableComparable<UidAndStatusTime> {
		
		  public Long getUid() {
			return uid;
		}

		public void setUid(Long uid) {
			this.uid = uid;
		}

		public Long getDateTime() {
			return dateTime;
		}

		public void setDateTime(Long dateTime) {
			this.dateTime = dateTime;
		}

		private Long uid = 0l;
		  private Long dateTime = 0l;

		  public  UidAndStatusTime (Long uid, Long dateTime) {
			  this.uid = uid;
			  this.dateTime = dateTime;
		  }
		  
		  public  UidAndStatusTime () {
		  }
		 
		  @Override
		  public void readFields(DataInput in) throws IOException {
			  uid = in.readLong();
			  dateTime = in.readLong();
		  }
		  @Override
		  public void write(DataOutput out) throws IOException {
		    out.writeLong(uid);
		    out.writeLong(dateTime);
		  }
		  
//		  @Override
//		  public int hashCode() {
//		    return orgUid.hashCode() + uid.hashCode();
//		  }
//		  
//		 
//		  @Override
//		  public boolean equals(Object uid) {
//		    if (uid instanceof OrgUidAndUid) {
//		    	OrgUidAndUid r = (OrgUidAndUid) uid;
//		    	return r.getOrgUid() == this.orgUid && r.getUid() == this.uid;
//		    } else {
//		      return false;
//		    }
//		  }
		  
		  //这里的代码是关键，因为对key排序时，调用的就是这个compareTo方法
		  @Override
		  public int compareTo(UidAndStatusTime o) {
		    if (uid.longValue() != o.uid.longValue()) {
		    	return uid.compareTo(o.uid);
		    } else {
		    	return -this.dateTime.compareTo(o.dateTime);
		    }
		  }
		 
		}
	
	/**
	 * 聚集
	 */
	public static class GroupingComparator  extends WritableComparator {
		public GroupingComparator() {
			// register comparator
			super(UidAndStatusTime.class, true);
		}

		@Override
		@SuppressWarnings("all")
		public int compare(WritableComparable a, WritableComparable b) {
			UidAndStatusTime o1 = (UidAndStatusTime) a;
			UidAndStatusTime o2 = (UidAndStatusTime) b;
			return o1.getUid().compareTo(o2.getUid());
		}

	}
	/**
	 * 排序
	 */
	public static class OrgUidAndUidComparator extends WritableComparator {
		public OrgUidAndUidComparator() {
			super(UidAndStatusTime.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			UidAndStatusTime o1 = (UidAndStatusTime) a;
			UidAndStatusTime o2 = (UidAndStatusTime) b;
			
			if (o1.getUid().longValue() != o2.getUid().longValue()) {
				return o1.getUid().compareTo(o2.getUid());
			} else {
				return -o1.getDateTime().compareTo(o2.getDateTime());
			}
			
		}
	}
	
	public static  class PartitionByOrgUid extends
	Partitioner<UidAndStatusTime, Text> {
	
		@Override
		public int getPartition(UidAndStatusTime key, Text value, int numPartitions) {
			return (key.uid.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		UserStatusLimitSize(args);
	}
}




