package merge;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.util.Date;
import java.util.Iterator;

import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

/**
 * 输入路径： clearOriginalStatus clearForwardStatus
 * /scribedata/climb2.0/status_forward /scribedata/climb2.0/original_status
 * 
 * 通过sid进行去重 根据sid对以上数据进行去重合并
 */
public class MergeStatus extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(MergeStatus.class);

	/***************** map ********************/
	public static class Map extends Mapper<Object, Text, Text, Text> {

		private static Text key = new Text();
		private static Text value = new Text();
		FileSystem fs = null;
		OutputStream out = null;

		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			String dst = "hdfs://hadoop-1:9000";
			Configuration conf = new Configuration();

			conf.set("fs.hdfs.impl",
					org.apache.hadoop.hdfs.DistributedFileSystem.class
							.getName());
			conf.set("fs.file.impl",
					org.apache.hadoop.fs.LocalFileSystem.class.getName());

			fs = FileSystem.get(URI.create(dst), conf);
			out = fs.create(new Path("/user/guest/"
					+ InetAddress.getLocalHost().getHostName() + new Date().getTime()+"_map.log"),true);
		}

		public void map(Object keyIn, Text valueIn, Context context) {
			if (null == valueIn || valueIn.getLength() <= 0) {
				return;
			}
			try {
				String json = valueIn.toString();
				JSONObject jsonObject = JSONObject.fromObject(json);
				if (jsonObject.containsKey("sid")) {
					String sid = jsonObject.getString("sid");
					out.write((sid+"\n").getBytes());
					key.set(sid);
					value.set(valueIn);
					context.write(key, value);
				}
			} catch (Exception e) {
				logger.error(" Mapper.map. ERROR : uid=" + key
						+ "or org_uid = " + valueIn);
			}
		}

		@Override
		protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}

	/***************** reduce ********************/
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		FileSystem fs = null;
		OutputStream out = null;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) {
			for (Iterator<Text> iterator = values.iterator(); iterator
					.hasNext();) {
				Text status = iterator.next();
				try {
					context.write(status, new Text(""));
					out.write((key.toString()+"\n").getBytes());
					break;
				} catch (Exception e) {
					logger.error(" reducer.reduce. ERROR : sid=" + key);
				}
			}
		}

		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = new Configuration();
			String dst = "hdfs://hadoop-1:9000";
			conf.set("fs.hdfs.impl",
					org.apache.hadoop.hdfs.DistributedFileSystem.class
							.getName());
			conf.set("fs.file.impl",
					org.apache.hadoop.fs.LocalFileSystem.class.getName());

			fs = FileSystem.get(URI.create(dst), conf);
			out = fs.create(new Path("/user/guest/"
					+ InetAddress.getLocalHost().getHostName() + new Date().getTime()+"_reduce.log"),true);
		}

		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}

	/***************** main ********************/

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		// class name.
		String shortName = "mergetest";
		String inputPath = "/exp_out/merge_status_1";
		String outputFilePath = "/user/guest/result";
		if (args.length > 2) {
			inputPath = args[0];
			outputFilePath = args[1];
		}
		
		String[] inputPaths = inputPath.split(",");


		//ConfigurationUtils.setThirdJars(conf);

		Job job = Job.getInstance(conf, shortName);

		job.setJarByClass(MergeStatus.class);

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

		
		FileOutputFormat.setOutputPath(job, new Path(outputFilePath));
		job.waitForCompletion(true);
		return 0;
	}

	public static int mergeStatus(String[] args) {
		// run job
		int code = 0;
		try {
			code = ToolRunner.run(new Configuration(), new MergeStatus(), args);
		} catch (Exception e) {
			logger.error(" error. ", e);
		}
		logger.info(" main [ end = " + System.currentTimeMillis() + " ]. code="
				+ code);
		return code;
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		mergeStatus(args);
	}
}
