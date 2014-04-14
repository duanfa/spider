package parser;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import parser.bean.SeenMovie;
import spider.Constant;

public class UserSeenMovieParser {
	public static void main(String[] args) throws IOException {
		String path = "/home/duanfa/Desktop/tmp/wish.html";
		File input = new File(path);
		Document doc = Jsoup.parse(input, "UTF-8");
		parseUserSeenMovieParser(doc);
	}

	public static Set<SeenMovie> parseUserSeenMovieParser(Document doc) throws IOException {
		Set<SeenMovie> seenMovies = new HashSet<SeenMovie>();

		String userId = "";

		for (Element e : doc.getElementsByAttributeValue("class", "mod")) {
			for (Element a : e.getElementsByTag("a")) {
				int i = 0;
				String[] urls = a.attr("href").split("/");
				for (String s : urls) {
					if ("people".equals(urls[i++])) {
						// System.out.println(urls[i]);
						userId = urls[i];
						break;
					}
				}

			}
		}
		for (Element e : doc.getElementsByAttributeValue("class", "info")) {
			SeenMovie movie = new SeenMovie();
			movie.setUserId(userId);
			for (Element r : e.getElementsByAttributeValue("class", "date")) {
//				System.out.println(r.text());
				try {
					movie.setDate(Constant.day_sdf.parse(r.text()));
				} catch (ParseException e1) {
					e1.printStackTrace();
				}
			}
			for (Element r : e.getElementsByAttributeValue("class", "title")) {
				for (Element t : r.getElementsByTag("a")) {
					int i = 0;
					String[] urls = t.attr("href").split("/");
					for (String s : urls) {
						if ("subject".equals(urls[i++])) {
							System.out.println(urls[i]);
							movie.setMovieId(urls[i]);
							break;
						}
					}
				}

			}
		}

		return seenMovies;
	}

}
