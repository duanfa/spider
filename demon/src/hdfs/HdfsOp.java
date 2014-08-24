package hdfs;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsOp {
	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		readFromHdfs();
	}

	private static void readFromHdfs() throws FileNotFoundException,
			IOException {
		String dst = "hdfs://offline:9000";
		Configuration conf = new Configuration();

		/*conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());*/

		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FileStatus[] stats = fs.listStatus(new Path("/"));
		System.out.println(stats.length);
		FSDataInputStream hdfsInStream = fs.open(new Path(dst));
		hdfsInStream.skip(100);
		fs.close();
	}
}
