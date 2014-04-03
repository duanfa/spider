

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jsoup.Jsoup;

public class Spider {
	public static void main(String[] args) throws IOException {
		boolean b = true;
		//b = false;
		if (b) {
			ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
			cachedThreadPool.execute(new UrlSaveDemon());
			cachedThreadPool.execute(new HttpDemon());
			cachedThreadPool.execute(new HttpDemon());
			cachedThreadPool.execute(new HttpDemon());
			System.out.println("started!!!");
		} else {
			String url = "http://movie.douban.com/subject/21941804/?from=showing";
			url = "http://www.douban.com/note/340388515";
			String html = "";
			//Element body = Jsoup.connect(url).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.64 Safari/537.31").header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8").header("Accept-Language", "zh-cn")
			//		.get().body();
			//System.out.println(body.childNodeSize());
			html = Jsoup.connect(url).userAgent("Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:28.0) Gecko/20100101 Firefox/28.0").header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8").header("Accept-Language", "zh-cn")
					.header("Cookie","bid=\"NmOUYwtrguc\"; viewed=\"2299730_4817792_1402410_4753298_25709562_4141733_1761909_1402576\"; __utma=30149280.871955665.1378794346.1396488248.1396493179.40; __utmz=30149280.1396415046.37.5.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/; ll=\"108288\"; __utmv=30149280.429; __utmc=30149280; dbcl2=\"4293463:kAydnRA4Mps\"; ck=\"NMsQ\"; push_noty_num=0; push_doumail_num=0; __utmb=30149280.20.10.1396493179; ct=y, value").execute().body();
			System.out.println(html);
		}
	}
}
