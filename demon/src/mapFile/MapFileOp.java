package mapFile;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class MapFileOp {

	private static void write() throws IOException {
		Configuration conf = new Configuration();
		URI uri = URI.create("file:///root/mapFile.map");
		FileSystem fs = FileSystem.get(uri, conf);
		MapFile.Writer writer = null;
		writer = new MapFile.Writer(conf, fs, uri.getPath(), Text.class, Text.class);

		//ͨ��writer���ĵ���д���¼  
		writer.append(new Text("key"), new Text("value"));
		IOUtils.closeStream(writer);//�ر�write��  
	}

	private static void read() throws IOException {
		Configuration conf = new Configuration();
		URI uri = URI.create("file:///root/mapFile.map");
		FileSystem fs = FileSystem.get(uri, conf);
		MapFile.Reader reader = null;
		reader = new MapFile.Reader(fs, uri.getPath(), conf);

		//ͨ��writer���ĵ���д���¼  
		Text key = new Text();
		Text value = new Text();
		while (reader.next(key, value)) {
			System.out.println(key);
			System.out.println(value);
		}
		IOUtils.closeStream(reader);//�ر�write��  
	}

	public static void main(String[] args) throws IOException {
		write();
		read();
	}
}
