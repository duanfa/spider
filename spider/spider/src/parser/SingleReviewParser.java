package parser;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import parser.bean.Reply;
import parser.bean.Review;
import spider.Constant;

public class SingleReviewParser {
	public static void main(String[] args) throws IOException {
		String path = "/home/duanfa/Desktop/tmp/1396554496441.html";
		File input = new File(path);
		Document doc = Jsoup.parse(input, "UTF-8");
		parseReview(doc);
	}

	public static Review parseReview(Document doc) throws IOException {
		Review review = new Review();

		for (Element e : doc.getElementsByAttributeValue("class", "side-back")) {
			for (Element a : e.getElementsByTag("a")) {
				int i = 0;
				String[] urls = a.attr("href").split("/");
				for (String s : urls) {
					if ("subject".equals(urls[i++])) {
						// System.out.println(urls[i]);
						review.setMovieId(urls[i]);
						break;
					}
				}

			}
		}
		for (Element e : doc.getElementsByAttributeValue("property", "v:summary")) {
			// System.out.println("v:summary:"+e.text());
			review.setTitle(e.text());
		}
		for (Element e : doc.getElementsByAttributeValue("property", "v:description")) {
			// System.out.println("v:description:"+e.text());
			review.setDetail(e.text());
		}
		for (Element e : doc.getElementsByAttributeValue("property", "v:dtreviewed")) {
			// System.out.println("v:dtreviewed:"+e.text());
			try {
				review.setDate(Constant.time_sdf.parse(e.text()));
			} catch (ParseException e1) {
				e1.printStackTrace();
			}
		}
		for (Element e : doc.getElementsByAttributeValue("property", "v:reviewer")) {
			int i = 0;
			String[] urls = e.parent().attr("href").split("/");
			for (String s : urls) {
				if ("people".equals(urls[i++])) {
					// System.out.println(urls[i]);
					review.setUserId(urls[i]);
					break;
				}
			}

			String[] starValus = e.parent().nextElementSibling().attr("class").split(" ");
			review.setStart(Integer.parseInt(starValus[0].substring(7)));
			// System.out.println(review.getStart());
		}
		for (Element e : doc.getElementsByAttributeValue("class", "btn-useful j a_show_login")) {
			review.setUseful(Integer.parseInt(e.nextElementSibling().text()));
			//System.out.println("useful:" + review.getUseful());
		}
		for (Element e : doc.getElementsByAttributeValue("class", "btn-unuseful j a_show_login")) {
			review.setUnUseful(Integer.parseInt(e.nextElementSibling().text()));
			//System.out.println("unUseful:" + review.getUnUseful());
		}

		List<Reply> replys = new ArrayList<Reply>();

		for (Element e : doc.getElementsByAttributeValue("class", "content report-comment")) {
			Reply reply = new Reply();
			for (Element a : e.getElementsByAttributeValue("class", "author")) {
				for (Element s : a.getElementsByTag("span")) {
					//System.out.println(s.text());
					try {
						reply.setDate(Constant.time_sdf.parse(s.text()));
					} catch (ParseException e1) {
						e1.printStackTrace();
					}
				}
				for (Element s : a.getElementsByTag("a")) {
					//System.out.println(s.attr("href"));
					int i = 0;
					String[] urls = s.attr("href").split("/");
					for (String k : urls) {
						if ("people".equals(urls[i++])) {
							//System.out.println(urls[i]);
							reply.setUserId(urls[i]);
							break;
						}
					}
				}
			}
			for (Element p : e.getElementsByTag("p")) {
				reply.setDetail(p.text());
				//System.out.println(p.text());
			}
			replys.add(reply);
//			System.out.println("comments:"+e.text());
		}
		review.setReplys(replys);

		return review;
	}

}
