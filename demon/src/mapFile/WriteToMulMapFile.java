package mapFile;

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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
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

/**
 * 
 * hadoop jar MergeUserLastestStatus.jar
 * com.st.mapreduce2.exp.MergeUserLastestStatus 原始输入路径1,原始输入路径2....
 * 增量抓取路径1,增量抓取路径2..... 输出路径 reduce个数 用户的微博个数
 */
public class WriteToMulMapFile extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(WriteToMulMapFile.class);

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
		private MapFile.Writer writer;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			lastestStatusSize = context.getConfiguration().getInt("writetoMapfile", 800);
			super.setup(context);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			writer = new MapFile.Writer(context.getConfiguration(), fs,"/user/guest/mapFile.map", LongWritable.class,Text.class);
		}
		@Override
		protected void cleanup(Reducer<UidAndStatusTime, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			IOUtils.closeStream(writer);// 关闭write流
		}

		@Override
		public void reduce(UidAndStatusTime key, Iterable<Text> values, Context context) {

			sids.clear();
			int count = 0;
			StringBuffer all = new StringBuffer();
			System.out.println("reduce:"+context.getConfiguration().get("mapreduce.job.reduces")+" partionid:"+((key.uid.hashCode() & Integer.MAX_VALUE) % Integer.parseInt(context.getConfiguration().get("mapreduce.job.reduces"))));
			for (Iterator<Text> iterator = values.iterator(); iterator.hasNext();) {
				try {
					if (count >= lastestStatusSize) {
						break;
					}
					String json = iterator.next().toString();
					all.append(json);
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
			// 通过writer向文档中写入记录
			try {
				writer.append(new LongWritable(key.getUid()), new Text(all.toString()));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/***************** main ********************/

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		String shortName = "insertmapFile-"+ new Date().getDay()+ new Date().getHours() + new Date().getMinutes();
		String inputPath = "/user/guest/statu_source";
		String outputFilePath = "/user/guest/insertToMapFile" + new Date().getHours() + new Date().getMinutes();
		if (args.length == 2) {
			inputPath = args[0];
			outputFilePath = args[1];
		}

		String[] inputPaths = inputPath.split(",");
		// class name.
		
		Job job = Job.getInstance(conf, shortName);

		job.setJarByClass(WriteToMulMapFile.class);
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
			code = ToolRunner.run(new Configuration(), new WriteToMulMapFile(), args);

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
			System.out.println("--------------------numPartitions:"+numPartitions+"-----------------------------");
			return (key.uid.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		UserStatusLimitSize(args);
	}
}
