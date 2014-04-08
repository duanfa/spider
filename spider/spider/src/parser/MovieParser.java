package parser;

import java.io.File;
import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class MovieParser {
///home/duanfa/Desktop/tmp/201404081205/movie.douban.com/subject/1761848/1396929955845.html
	public static void main(String[] args) throws IOException {
		String path = "/home/duanfa/Desktop/tmp/201404081205/movie.douban.com/subject/1761848/1396929955845.html";
		File input = new File(path);
		Document doc = Jsoup.parse(input, "UTF-8");
		for(Element e:doc.getElementsByAttributeValue("property", "v:itemreviewed")){
			System.out.println(e.toString());
		}
	}
	
}
