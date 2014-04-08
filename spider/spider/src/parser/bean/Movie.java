package parser.bean;

import java.io.File;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;

public class Movie {
///home/duanfa/Desktop/tmp/201404081205/movie.douban.com/subject/1761848/1396929955845.html
	
	public static void main(String[] args) {
		String path = "/home/duanfa/Desktop/tmp/201404081205/movie.douban.com/subject/1761848/1396929955845.html";
		Document document = null;
		try {
			SAXReader saxReader = new SAXReader();
			document = saxReader.read(new File(path));  
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		System.out.println(document.toString());
	}
}
