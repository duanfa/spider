package hdfs.globStatus;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class GetFileStatus {
	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		
		FileStatus[] stats = null;
		try {
			FileSystem hdfs = FileSystem.get(URI.create("hdfs://offline:9000/"), conf);
			Path listf = new Path("/scribedata/climb2.0/user/*2014-07-22*");
//			stats = hdfs.listStatus(listf);
			stats = hdfs.globStatus(listf);
			System.out.println("hadoop 文件系统加载成功");
		} catch (IOException e2) {
			System.out.println("hadoop 文件系统加载失败");
			e2.printStackTrace();
		}
		System.out.println(stats.length);
		for(FileStatus statu:stats){
			System.out.println(statu.getPath());
		}
	}
}
