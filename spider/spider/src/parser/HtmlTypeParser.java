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

public class HtmlTypeParser {

	public enum HtmlType {
		movie, movieReview, shortReview, review, userReview, userSeenMovies
	}

	// /home/duanfa/Desktop/tmp/201404081205/movie.douban.com/subject/1761848/1396929955845.html
	public static void main(String[] args) throws IOException {
		String moviepath = "/douban/douban_bak/201404040927/movie.douban.com/subject/10833923/1396574878343.html";
		String movieReview = "/home/duanfa/Desktop/tmp/moviereview.html";
		String shortreview = "/home/duanfa/Desktop/tmp/shortreview.html";
		String singleReview = "/home/duanfa/Desktop/tmp/1396554496441.html";
		String userReview = "/home/duanfa/Desktop/tmp/userreview.html";
		String userseen = "/home/duanfa/Desktop/tmp/wish.html";
		File input = new File(userseen);
		Document doc = Jsoup.parse(input, "UTF-8");
		htmlTypeParser(doc);
	}

	public static HtmlType htmlTypeParser(Document doc) throws IOException {
		HtmlType result = null;
		for(Element e:doc.getElementsByAttributeValue("property","v:itemreviewed")){
			result = HtmlType.movie;
			System.out.println(result);
		}
		for(Element e:doc.getElementsByAttributeValue("class","side-copyright")){
			if(e.text().indexOf("本评论版权属于作者")>-1){
				result = HtmlType.review;
				System.out.println(result);
			}
		}
		for(Element e:doc.getElementsByTag("h1")){
			if(e.text().indexOf("的影评")>-1){
				for(Element d:doc.getElementsByAttributeValue("class","green_tab clearfix")){
					if(d.text().indexOf("热门影评")>-1){
						result = HtmlType.movieReview;
						System.out.println(result);
					}
				}
			}
		}
		for(Element e:doc.getElementsByTag("h1")){
			if(e.text().indexOf("的影评")>-1){
				for(Element d:doc.getElementsByAttributeValue("id","db-usr-profile")){
				//	if(d.text().indexOf("热门影评")>-1){
						result = HtmlType.userReview;
						System.out.println(result);
					//}
				}
			}
		}
		for(Element e:doc.getElementsByTag("h1")){
			if(e.text().indexOf("看过的")>-1||e.text().indexOf("在看的")>-1||e.text().indexOf("想看的")>-1){
					result = HtmlType.userSeenMovies;
					System.out.println(result);
			}
		}
		for(Element e:doc.getElementsByTag("h1")){
			if(e.text().indexOf("短评")>-1){
				result = HtmlType.shortReview;
				System.out.println(result);
			}
		}
		return result;
	}

}
