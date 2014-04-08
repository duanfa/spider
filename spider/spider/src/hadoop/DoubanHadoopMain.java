package hadoop;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DoubanHadoopMain {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "douban");
		job.setJarByClass(DoubanHadoopMain.class);
		job.setInputFormatClass(DoubanFindFileInputFormat.class);
		job.setMapperClass(DoubanMapper.class);
		job.setCombinerClass(DoubanReducer.class);
		job.setReducerClass(DoubanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
//		DoubanFindFileInputFormat.addInputPath(job, "hdfs://server128:9000/douban");
		DoubanFindFileInputFormat.addInputPath(job, "/douban");
		FileOutputFormat.setOutputPath(job, new Path("hdfs://server128:9000/doubanOut"+new Date().getMinutes()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
