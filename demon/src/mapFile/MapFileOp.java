package mapFile;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class MapFileOp {

	private static void write() throws IOException {
		Configuration conf = new Configuration();
		URI uri = URI.create("file:///root/mapFile.map");
		FileSystem fs = FileSystem.get(uri, conf);
		MapFile.Writer writer = null;
		writer = new MapFile.Writer(conf, fs, uri.getPath(), Text.class, Text.class);
		//通过writer向文档中写入记录  
		writer.append(new Text("key"), new Text("value"));
		writer.append(new Text("key2"), new Text("value2.0"));
		writer.append(new Text("key2"), new Text("value2.1"));
		writer.append(new Text("key2"), new Text("value2.2"));
		writer.append(new Text("key3"), new Text("value3"));
		writer.append(new Text("key4"), new Text("value4"));
		IOUtils.closeStream(writer);//关闭write流  
	}

	private static void read() throws IOException {
		Configuration conf = new Configuration();
		URI uri = URI.create("hdfs://master:9000/user/duanfa/mapFile0");
		FileSystem fs = FileSystem.get(uri, conf);
		MapFile.Reader reader = null;
		reader = new MapFile.Reader(fs, uri.getPath(), conf);

		//通过writer向文档中写入记录  
		LongWritable key = new LongWritable(1981121952);
		Text value = new Text();
		while (reader.next(key, value)) {
			System.out.println(key);
			System.out.println(value);
		}
		System.out.println("--------------------");
		IOUtils.closeStream(reader);//关闭write流  
	}

	public static void main(String[] args) throws IOException {
		//write();
		read();
	}
}
