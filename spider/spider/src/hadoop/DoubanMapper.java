package hadoop;

import hadoop.hbase.DoubanOperater;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import parser.HrefParser;

public class DoubanMapper extends Mapper<Text, Text, Text, Text> {

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		Document doc = Jsoup.parse(value.toString(), "UTF-8");
		parseHref(doc);
		// context.write(arg0, arg1);;
	}
	public static void main(String[] args) throws IOException {
		String path = "/douban/douban_bak/201404040927/movie.douban.com/subject/10833923/1396574878343.html";
		File input = new File(path);
		Document doc = Jsoup.parse(input, "UTF-8");
		//parseHref(doc);
		
	}
	private static void parseHref(Document doc) {
		Set<String> hrefs = HrefParser.parseHref(doc);
		for (String href : hrefs) {
			String id = "";
			for (String category : HrefParser.categorys) {
				int i = 0;
				String[] urls = href.split("/");
				for (String s : urls) {
					if (category.equals(urls[i++])) {
						id = urls[i];
						if(DoubanOperater.saveHref(category, id, href)==0){
							break;
						}else{
							System.out.println("save href error----:"+category+":"+id+":"+href);
						}
					}
				}
				if(StringUtils.isNotBlank(id)){
					break;
				}
			}
			
		}
	}
}