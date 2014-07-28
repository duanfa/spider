package mergeLatest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import net.sf.json.JSONObject;

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

import com.st.util.ConfigurationUtils;

/**
 * 
 * hadoop jar MergeUserLastestStatus.jar
 * com.st.mapreduce2.exp.MergeUserLastestStatus 原始输入路径1,原始输入路径2....
 * 增量抓取路径1,增量抓取路径2..... 输出路径 reduce个数 用户的微博个数
 */
public class MergeUserLastestStatus extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(MergeUserLastestStatus.class);

	/***************** map ********************/
	public static class Map extends Mapper<Object, Text, UidAndStatusTime, Text> {

		private static Text value = new Text();
		private static SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", java.util.Locale.ENGLISH);

		public void map(Object keyIn, Text valueIn, Context context) {

			if (null == valueIn || valueIn.getLength() <= 0) {
				return;
			}
			try {
				String json = valueIn.toString();

				JSONObject jsonObject = JSONObject.fromObject(json);
				if (jsonObject.containsKey("uid") && jsonObject.containsKey("created_at") && jsonObject.containsKey("sid")) {
					long uid = jsonObject.getLong("uid");
					String createAt = jsonObject.getString("created_at");

					Date date = format.parse(createAt);
					UidAndStatusTime key = new UidAndStatusTime(uid, date.getTime());
					value.set(valueIn);
					context.write(key, value);
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
		protected void setup(Context context) throws IOException, InterruptedException {
			lastestStatusSize = context.getConfiguration().getInt("lastestStatus", 800);
			super.setup(context);
		}

		@Override
		public void reduce(UidAndStatusTime key, Iterable<Text> values, Context context) {

			sids.clear();
			int count = 0;
			for (Iterator<Text> iterator = values.iterator(); iterator.hasNext();) {
				try {
					if (count >= lastestStatusSize) {
						break;
					}
					String json = iterator.next().toString();
					JSONObject jsonObject = JSONObject.fromObject(json);
					Long sid = jsonObject.getLong("sid");

					if (!sids.contains(sid)) {
						sids.add(sid);
						count = count + 1;
						keyout.set(json);
						valout.set("");
						//context.write(keyout, valout);
					} else {
						continue;
					}
				} catch (Exception e) {
					logger.error(" reducer.reduce. ERROR : uid=" + key);
				}
			}
		}
	}

	/***************** main ********************/

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		String shortName = "merge800-"+ new Date().getDay()+ new Date().getHours() + new Date().getMinutes();
		String inputPath = "/user/guest/original_status";
		String outputFilePath = "/user/guest/hbaseInsertResult" + new Date().getHours() + new Date().getMinutes();
		if (args.length == 2) {
			inputPath = args[0];
			outputFilePath = args[1];
		}

		String[] inputPaths = inputPath.split(",");
		// class name.
		
		Job job = Job.getInstance(conf, shortName);

		job.setJarByClass(MergeUserLastestStatus.class);
		job.setNumReduceTasks(9);
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

		for (int i = 0; i < inputPaths.length; i++) {
			String pathString = inputPaths[i];
			FileInputFormat.addInputPath(job, new Path(pathString));
		}

		FileOutputFormat.setOutputPath(job, new Path(outputFilePath));
		job.waitForCompletion(true);
		return 0;
	}


	public static int UserStatusLimitSize(String[] args) {
		// run job
		int code = 0;
		try {
			code = ToolRunner.run(new Configuration(), new MergeUserLastestStatus(), args);

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

		public UidAndStatusTime(Long uid, Long dateTime) {
			this.uid = uid;
			this.dateTime = dateTime;
		}

		public UidAndStatusTime() {
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

		// 这里的代码是关键，因为对key排序时，调用的就是这个compareTo方法
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
	public static class GroupingComparator extends WritableComparator {
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

	public static class PartitionByOrgUid extends Partitioner<UidAndStatusTime, Text> {

		@Override
		public int getPartition(UidAndStatusTime key, Text value, int numPartitions) {
			return (key.uid.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		UserStatusLimitSize(args);
	}
}
