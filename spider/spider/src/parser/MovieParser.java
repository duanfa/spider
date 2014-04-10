package parser;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import parser.bean.Movie;
import parser.bean.Review;
import parser.bean.Review.ReviewType;
import spider.Constant;

public class MovieParser {
	// /home/duanfa/Desktop/tmp/201404081205/movie.douban.com/subject/1761848/1396929955845.html
	public static void main(String[] args) throws IOException {
		// String path =
		// "/douban/douban_bak/201404040927/movie.douban.com/subject/10833923/1396574878343.html";
		String path = "D:/douban/201404092202/movie.douban.com/subject/4860078/1397052125619.html";
		parseMovie(path);
	}

	public static void parseMovie(String path) throws IOException {
		Movie movie = new Movie();
		File input = new File(path);
		Document doc = Jsoup.parse(input, "UTF-8");
		for (Element e : doc.getElementsByAttributeValue("property",
				"v:itemreviewed")) {
			// System.out.println("name:"+e.text());
			movie.setName(e.text());
		}
		for (Element e : doc.getElementsByAttributeValue("rel", "v:directedBy")) {
			// System.out.println("v:directedBy:"+e.text());
			List<String> dirs = new ArrayList<String>();
			for (String s : e.text().split("/")) {
				dirs.add(s);
				// System.out.println("direct:"+s);
			}
			movie.setDirectors(dirs);
		}
		for (Element e : doc.getElementsByAttributeValue("property",
				"v:summary")) {
			// System.out.println("detail:"+e.text());
			movie.setDetail(e.text());
		}
		for (Element e : doc.getElementsByAttributeValue("property",
				"v:average")) {
		// System.out.println("setStartValue:"+e.text());
			movie.setStartValue(Float.parseFloat(e.text()));
		}
		for (Element e : doc.getElementsByAttributeValue("property",
				"v:votes")) {
			//System.out.println("setStartAll:"+e.text());
			movie.setStartAll(Integer.parseInt(e.text()));
		}
		for (Element e : doc.getElementsByAttributeValue("rel",
				"v:rating")) {
			String[] stars = e.ownText().split("%");
			movie.setStart1(Float.parseFloat(stars[0]));
			movie.setStart2(Float.parseFloat(stars[1]));
			movie.setStart3(Float.parseFloat(stars[2]));
			movie.setStart4(Float.parseFloat(stars[3]));
			movie.setStart5(Float.parseFloat(stars[4]));
			//movie.setStartAll(Integer.parseInt(e.text()));
		}
		Set<String> tags = new HashSet<String>();
		for (Element e : doc.getElementsByAttributeValue("class",
				"tags-body")) {
			for (Element t : e.getElementsByTag("a")) {
//				System.out.println(t.text());
				tags.add(t.text());
			}
		}
		movie.setTags(tags);
		List<Review> short_reviews = new ArrayList<Review>();
		for (Element e : doc.getElementsByAttributeValue("class",
				"comment-item")) {
			Review review = new Review();
			review.setType(ReviewType.SHORT);
			for (Element title : e.getElementsByAttributeValue("class",
					"votes pr5")) {
				review.setUseful(Integer.parseInt(title.ownText()));
			}
			for (Element title : e.getElementsByTag("input")) {
				review.setId(title.attr("value"));
			}
			for (Element title : e.getElementsByAttributeValue("class",
					"comment-info")) {
				for (Element u : title.children()) {
					for (Element s : u.getElementsByTag("span")) {
						if (s.attr("class").length() > 0) {
							String[] stars = s.attr("class").split(" ");
							review.setStart(Integer.parseInt(stars[0]
									.substring(7)));
							// System.out.println("star:" +
							// Integer.parseInt(stars[0].substring(7)));
						}
						if (s.ownText().length() > 0) {
							try {
								review.setDate(Constant.day_sdf.parse(s
										.ownText()));
							} catch (ParseException e1) {
								e1.printStackTrace();
							}
							// System.out.println("time:" + s.ownText());
						}
					}
				}
				for (Element u : title.getElementsByTag("a")) {
					int i = 0;
					String[] urls = u.attr("href").split("/");
					for (String s : urls) {
						if ("people".equals(urls[i++])) {
							review.setUserId(urls[i]);
							break;
						}
					}
				}
			}
			short_reviews.add(review);
		}
		movie.setShortReviews(short_reviews);

		List<Review> reviews = new ArrayList<Review>();
		for (Element e : doc.getElementsByAttributeValue("class", "review")) {
			Review review = new Review();
			boolean istitle = false;
			for (Element title : e.getElementsByAttributeValue("onclick",
					"moreurl(this, {from: 'review-hottest'})")) {
				if (!istitle) {
					review.setTitle(title.text());
					// System.out.println("title:"+title.text());
				}
				istitle = true;
			}
			for (Element replyeNum : e.getElementsByAttributeValue("class",
					"review-bd")) {
				for (Element no : replyeNum.getElementsByAttributeValue(
						"onclick", "moreurl(this, {from: 'review-hottest'})")) {
					review.setReplyNum(Integer.parseInt(no.text().substring(0,
							no.text().indexOf("回复"))));
					// System.out.println(review.getReplyNum());
					String[] url = no.attr("href").split("/");
					review.setId(url[url.length - 1]);
					// System.out.println("id:"+review.getId());
					break;
				}
			}
			for (Element use : e.getElementsByAttributeValue("class",
					"review-short-ft")) {
				for (Element no : use.getElementsByTag("span")) {
					String[] usenum = no.text().split("/");
					review.setUseful(Integer.parseInt(usenum[0]));
					review.setUnUseful(Integer.parseInt(usenum[1]));
					// System.out.println("usenum:"+review.getUseful()+" unusenum:"+review.getUnUseful());
				}
			}
			for (Element user : e.getElementsByAttributeValue("class",
					"review-hd-info")) {
				for (Element u : user.getElementsByTag("a")) {
					String[] url = u.attr("href").split("/");
					review.setUserId(url[url.length - 1]);
					// System.out.println("userid:"+review.getUserId());
				}
				for (Element start : user.getElementsByTag("span")) {
					String star = start.attr("class");
					review.setStart(Integer.parseInt(star.substring(7)));
					// System.out.println("star:"+review.getStart());
				}
				try {
					review.setDate(Constant.time_sdf.parse(user.ownText()));
					// System.out.println("date:"+review.getDate());
				} catch (ParseException e1) {
					e1.printStackTrace();
				}
			}
		}
		movie.setReviews(reviews);

		Set<String> relativeMovies = new HashSet<String>();
		for (Element e : doc.getElementsByAttributeValue("class",
				"recommendations-bd")) {
			for (Element m : e.getElementsByTag("a")) {
				int i = 0;
				String[] urls = m.attr("href").split("/");
				for (String s : urls) {
					if ("subject".equals(urls[i++])) {
						relativeMovies.add(urls[i]);
						break;
					}
				}
			}
		}
		movie.setRelativeMovies(relativeMovies);
	}

}
