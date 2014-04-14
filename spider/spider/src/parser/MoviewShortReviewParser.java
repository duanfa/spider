package parser;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import parser.bean.Review;
import parser.bean.Review.ReviewType;
import spider.Constant;

public class MoviewShortReviewParser {
	public static void main(String[] args) throws IOException {
		String path = "/home/duanfa/Desktop/tmp/shortreview.html";
		File input = new File(path);
		Document doc = Jsoup.parse(input, "UTF-8");
		moviewShortReviewParser(doc);
	}

	public static Set<Review> moviewShortReviewParser(Document doc) throws IOException {
		Set<Review> short_reviews = new HashSet<Review>();
		for (Element e : doc.getElementsByAttributeValue("class", "comment-item")) {
			Review review = new Review();
			review.setType(ReviewType.SHORT);
			for (Element title : e.getElementsByAttributeValue("class", "votes pr5")) {
				// System.out.println("setUseful:" +
				// Integer.parseInt(title.ownText()));
				review.setUseful(Integer.parseInt(title.ownText()));
			}
			for (Element title : e.getElementsByTag("input")) {
				// System.out.println("id:" + title.attr("value"));
				review.setId(title.attr("value"));
			}
			for (Element title : e.getElementsByAttributeValue("class", "comment-info")) {
				for (Element u : title.children()) {
					for (Element s : u.getElementsByTag("span")) {
						if (s.attr("class").length() > 0) {
							String[] stars = s.attr("class").split(" ");
							review.setStart(Integer.parseInt(stars[0].substring(7)));
//							System.out.println("star:" + Integer.parseInt(stars[0].substring(7)));
						}
						if (s.ownText().length() > 0) {
							try {
								review.setDate(Constant.day_sdf.parse(s.ownText()));
							} catch (ParseException e1) {
								e1.printStackTrace();
							}
//							System.out.println("time:" + s.ownText());
						}
					}
				}
				for (Element u : title.getElementsByTag("a")) {
					int i = 0;
					String[] urls = u.attr("href").split("/");
					for (String s : urls) {
						if ("people".equals(urls[i++])) {
							review.setUserId(urls[i]);
//							System.out.println(urls[i]);
							break;
						}
					}
				}
			}
		}
		return short_reviews;

	}
}