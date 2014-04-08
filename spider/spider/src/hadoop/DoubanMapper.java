package hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;

public class DoubanMapper extends Mapper<Text, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		Document document;
		try {
			document = DocumentHelper.parseText(value.toString());
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		//document.getRootElement().elements();
	}
}