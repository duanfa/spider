package mapFile;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class MapFileOpHDFS {

	private static void write() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
		URI uri = URI.create("hdfs://offline:9000/user/guest/mapFile.map");
		FileSystem fs = FileSystem.get(uri, conf);
		MapFile.Writer writer = null;
		writer = new MapFile.Writer(conf, fs, "/user/guest/mapFile.map", Text.class,
				Text.class);

		// 通过writer向文档中写入记录
		writer.append(new Text("key"), new Text("value"));
		IOUtils.closeStream(writer);// 关闭write流
	}

	private static void read(long k) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
		URI uri = URI.create("hdfs://offline:9000");
		FileSystem fs = FileSystem.get(uri, conf);
		
		System.out.println("hadoop 文件系统加载成功");
		
		long start = System.currentTimeMillis();
		MapFile.Reader reader = null;
		reader = new MapFile.Reader(fs, "hdfs://offline:9000/user/guest/mapFile.map", conf);
		// 通过writer向文档中写入记录
		LongWritable key = new LongWritable(k);
		Text value = new Text();
		reader.get(key, value);
			System.out.println(key);
			System.out.println(value);
		
		IOUtils.closeStream(reader);// 关闭write流
		System.out.println("use:"+(System.currentTimeMillis()-start));
	}

	public static void main(String[] args) throws IOException {
		//write();
		read(Long.parseLong("3857532331"));
	}
}
