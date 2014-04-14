package parser;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import parser.bean.User;

public class UserReviewParser {
	public static void main(String[] args) throws IOException {
		String path = "/home/duanfa/Desktop/tmp/userreview.html";
		File input = new File(path);
		Document doc = Jsoup.parse(input, "UTF-8");
		userReviewParser(doc);
	}

	public static User userReviewParser(Document doc) throws IOException {
		
		User user = new User();
		
		Set<String> reviews =new HashSet<String>();;

		

		for (Element e : doc.getElementsByAttributeValue("style", "clear:both;")) {
			for (Element a : e.getElementsByTag("a")) {
				int i = 0;
				String[] urls = a.attr("href").split("/");
				for (String s : urls) {
					if ("subject".equals(urls[i++])) {
//						System.out.println(urls[i]);
						reviews.add(urls[i]);
						break;
					}
				}

			}
		}
		user.setReviews(reviews);
		for (Element e : doc.getElementsByAttributeValue("class", "starb")) {
			for (Element a : e.getElementsByTag("a")) {
				int i = 0;
				String[] urls = a.attr("href").split("/");
				for (String s : urls) {
					if ("people".equals(urls[i++])) {
//						System.out.println(urls[i]);
						user.setId(urls[i]);
						return user;
					}
				}
				
			}
		}

		return user;
	}

}
