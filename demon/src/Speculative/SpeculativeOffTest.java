package Speculative;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import Speculative.SpeculativeOnTest.UidAndSidKey;

public class SpeculativeOffTest  extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(SpeculativeOffTest.class);

	/***************** map ********************/
	public static class Map extends Mapper<Object, Text, UidAndSidKey, Text> {

		UidAndSidKey key;
		Text blankValue = new Text();
		Counter mapCount;

		@Override
		protected void setup(Mapper<Object, Text, UidAndSidKey, Text>.Context context) throws IOException, InterruptedException {
			super.setup(context);
			mapCount = context.getCounter("SpeculativeOnTest", "mapCount");
		}

		public void map(Object keyIn, Text valueIn, Context context) {
					try {
						JSONObject weiboObject = JSONObject.fromObject(valueIn.toString());
						String uid = weiboObject.getString("uid");
						String sid = weiboObject.getString("sid");
						key = new UidAndSidKey(Long.parseLong(uid), Long.parseLong(sid));
						context.write(key, valueIn);
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					mapCount.increment(1);
		}


		@Override
		protected void cleanup(Mapper<Object, Text, UidAndSidKey, Text>.Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}

	/***************** reduce ********************/
	public static class Reduce extends Reducer<UidAndSidKey, Text, Text, Text> {
		Text blankKey = new Text();
		Counter reduceCounter;

		@Override
		protected void setup(Reducer<UidAndSidKey, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			super.setup(context);
			reduceCounter = context.getCounter("SpeculativeOnTest", "reduceCounter");
		}

		@Override
		public void reduce(UidAndSidKey key, Iterable<Text> values, Context context) {
				for(Text t:values){
					reduceCounter.increment(1);
					try {
						context.write(blankKey,t);
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
		}


		@Override
		protected void cleanup(Reducer<UidAndSidKey, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}

	/***************** main ********************/

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		//conf.set("mapreduce.job.queuename","high");
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		String shortName = "SpeculateOFF" + new Date().getHours() + new Date().getMinutes();
		String inputPath = "/user/guest/original_status_test";
		String outputFilePath = "/user/guest/SpeculateOff" + new Date().getHours() + new Date().getMinutes();
		Job job = Job.getInstance(conf, shortName);
		if (args.length >0) {
			inputPath = args[0];
			outputFilePath = args[1];
		}

		String[] inputPaths = inputPath.split(",");

		job.setJarByClass(SpeculativeOffTest.class);

		// set mapper
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(UidAndSidKey.class);
		job.setMapOutputValueClass(Text.class);

		// set reducer
		job.setReducerClass(Reduce.class);
		// job.setOutputFormatClass(TextOutputFormat.class);

		job.setSortComparatorClass(OrgUidAndUidComparator.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setPartitionerClass(PartitionByUid.class);

		// set input-path
		for (int i = 0; i < inputPaths.length; i++) {
			String pathString = inputPaths[i];
			FileInputFormat.addInputPath(job, new Path(pathString));
		}

		FileOutputFormat.setOutputPath(job, new Path(outputFilePath));
		job.waitForCompletion(true);
		return 0;
	}

	public static int mergeStatus(String[] args) {
		// run job
		int code = 0;
		try {
			code = ToolRunner.run(new Configuration(), new SpeculativeOffTest(), args);
		} catch (Exception e) {
			logger.error(" error. ", e);
		}
		logger.info(" main [ end = " + System.currentTimeMillis() + " ]. code=" + code);
		return code;
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		mergeStatus(args);
	}

	public static class UidAndSidKey implements WritableComparable<UidAndSidKey> {
		private Long uid = 0l;
		private Long sid = 0l;

		public UidAndSidKey() {
		}

		public Long getUid() {
			return uid;
		}

		public void setUid(Long uid) {
			this.uid = uid;
		}

		public Long getSid() {
			return sid;
		}

		public void setSid(Long sid) {
			this.sid = sid;
		}

		public UidAndSidKey(Long uid, Long sid) {
			this.uid = uid;
			this.sid = sid;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			uid = in.readLong();
			sid = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(uid);
			out.writeLong(sid);
		}

		@Override
		public int compareTo(UidAndSidKey o) {
			if (uid.longValue() != o.uid.longValue()) {
				return uid.compareTo(o.uid);
			} else {
				return this.sid.compareTo(o.sid);
			}
		}

	}

	/**
	 * 
	 * @author root
	 *
	 */
	public static class GroupingComparator extends WritableComparator {
		public GroupingComparator() {
			// register comparator
			super(UidAndSidKey.class, true);
		}

		@Override
		@SuppressWarnings("all")
		public int compare(WritableComparable a, WritableComparable b) {
			UidAndSidKey o1 = (UidAndSidKey) a;
			UidAndSidKey o2 = (UidAndSidKey) b;
			return o1.getUid().compareTo(o2.getUid());
		}

	}

	/**
	 * 
	 * @author root
	 *
	 */
	public static class OrgUidAndUidComparator extends WritableComparator {
		public OrgUidAndUidComparator() {
			super(UidAndSidKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			UidAndSidKey o1 = (UidAndSidKey) a;
			UidAndSidKey o2 = (UidAndSidKey) b;

			if (o1.getUid().longValue() != o2.getUid().longValue()) {
				return o1.getUid().compareTo(o2.getUid());
			} else {
				return o1.getSid().compareTo(o2.getSid());
			}

		}
	}

	/**
	 * Partitioner
	 * 
	 * @author root
	 *
	 */
	public static class PartitionByUid extends Partitioner<UidAndSidKey, Text> {

		@Override
		public int getPartition(UidAndSidKey key, Text value, int numPartitions) {
			return (key.uid.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}


	
}
