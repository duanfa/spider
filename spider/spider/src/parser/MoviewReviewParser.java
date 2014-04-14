package parser;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import parser.bean.Movie;

public class MoviewReviewParser {
	public static void main(String[] args) throws IOException {
		String path = "/home/duanfa/Desktop/tmp/moviereview.html";
		File input = new File(path);
		Document doc = Jsoup.parse(input, "UTF-8");
		moviewReviewParser(doc);
	}

	public static Movie moviewReviewParser(Document doc) throws IOException {

		Movie movie = new Movie();

		Set<String> reviews = new HashSet<String>();


		for (Element e : doc.getElementsByAttributeValue("class", "j a_unfolder")) {
			int i = 0;
			String[] urls = e.attr("href").split("/");
			for (String s : urls) {
				if ("review".equals(urls[i++])) {
//					System.out.println(urls[i]);
					reviews.add(urls[i]);
					break;
				}
			}
		}
		movie.setReviews(reviews);
		for (Element e : doc.getElementsByAttributeValue("class", "green_tab clearfix")) {
			for (Element a : e.getElementsByTag("a")) {
				int i = 0;
				String[] urls = a.attr("href").split("/");
				for (String s : urls) {
					if ("subject".equals(urls[i++])) {
//						 System.out.println(urls[i]);
						movie.setId(urls[i]);
						return movie;
					}
				}

			}
		}

		return movie;
	}

}
