package hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class DoubanMapper extends Mapper<Text, Text, Text, Text> {


	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		Document doc = Jsoup.parse(value.toString(), "UTF-8");
	//	context.write(arg0, arg1);;
	}
}