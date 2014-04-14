package parser;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class HrefParser {

	static Set<String> include = new HashSet<String>();
	static {
		include.add("http://movie.douban.com/people");
		include.add("http://movie.douban.com/subject");
		include.add("http://movie.douban.com/subject");
		include.add("http://movie.douban.com/review");
		include.add("http://movie.douban.com/doulist");
		include.add("http://movie.douban.com/celebrity");
		include.add("/subject");
		include.add("/celebrity");
		// include.add("http://www.douban.com/group");
	}

	public static void main(String[] args) throws IOException {
		String path = "/douban/douban_bak/201404040927/movie.douban.com/subject/10833923/1396574878343.html";
		File input = new File(path);
		Document doc = Jsoup.parse(input, "UTF-8");
		parseHref(doc);
	}

	public static Set<String> parseHref(Document doc) throws IOException {
		Set<String> hrefs = new HashSet<>();
		for (Element e : doc.getElementsByTag("a")) {
			for (String head : include) {
				if (e.attr("href").indexOf(head)>-1) {
					System.out.println(e.attr("href"));
					hrefs.add(e.attr("href"));
					break;
				}
			}
		}
		return hrefs;
	}

}
