package spider;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.net.SocketTimeoutException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;

public class HttpDemon implements Runnable {

	static Set<String> include = new HashSet<String>();
	static Set<String> exclude = new HashSet<String>();
	
	static SimpleDateFormat sdf_m = new SimpleDateFormat("YYYYMMddHHmm");

	static Queue<String> queue = new ConcurrentLinkedQueue<String>();

	static {
		include.add("http://movie.douban.com/people");
		include.add("http://movie.douban.com/subject");
		include.add("http://movie.douban.com/review");
		include.add("http://movie.douban.com/people/annho/collect");
		include.add("http://movie.douban.com/doulist");
		include.add("http://movie.douban.com/celebrity");
		include.add("http://www.douban.com/group");

		exclude.add("?type=like#");
		exclude.add("remove_comment");
		exclude.add("www.douban.com/note/tags");
	}

	public static String fetchHtml(String url) {
		if (url == null || UrlSaveDemon.downloadUrls.contains(url)) {
			System.out.println("has been read url:" + url);
			try {
				System.out.println(Thread.currentThread().getId()+" sleep 10");
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		}else{
			try {
				System.out.println(Thread.currentThread().getId()+" sleep 3000");
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		String html = "";
		String path = "";
		String name = "";
		try {
			html =  Jsoup.connect(url).userAgent("Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:28.0) Gecko/20100101 Firefox/28.0").header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8").header("Accept-Language", "zh-cn")
			//.header("Cookie","bid=\"NmOUYwtrguc\"; viewed=\"2299730_4817792_1402410_4753298_25709562_4141733_1761909_1402576\"; __utma=30149280.871955665.1378794346.1396488248.1396493179.40; __utmz=30149280.1396415046.37.5.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/; ll=\"108288\"; __utmv=30149280.429; __utmc=30149280; dbcl2=\"4293463:kAydnRA4Mps\"; ck=\"NMsQ\"; push_noty_num=0; push_doumail_num=0; __utmb=30149280.20.10.1396493179; ct=y, value").execute().body();
			.header("Cookie","bid=\"mGHbje1iOus\"; ll=\"108288\"").execute().body();
			int last = url.lastIndexOf("/");
			path = "/douban/"+sdf_m.format(new Date())+url.substring(6, last);
			name = url.substring(last + 1);
			name = name.replaceAll("\\?", "_");
			if (name.length() < 1) {
				name = new Date().getTime() + "";
			}
			File dir = new File(path);
			if (!dir.exists()) {
				dir.mkdirs();
			}
			FileOutputStream fos = new FileOutputStream(path + File.separator
					+ name+".html");
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
			bw.write(html);
			bw.close();
			UrlSaveDemon.usedUrls.add(url);
			UrlSaveDemon.downloadUrls.add(url);
			System.out.println("success get url:" + url);
		} catch (Exception e) {
			if(e instanceof SocketTimeoutException){
				System.out.println("read time out url:" + url);
			}else{
				e.printStackTrace();
				System.out.println("illegeal url:" + url);
				UrlSaveDemon.illegalUrls.add(url);
				queue.add(url);
			}
		}
		return html;
	}

	public static Set<String> authHref(Set<String> hrefs) {
		Set<String> authhrefs = new HashSet<String>();
		for (String href : hrefs) {
			for (String in : include) {
				if (href.indexOf(in) > -1) {
					boolean isExclude = false;
					for (String ex : exclude) {
						if (href.indexOf(ex) > -1) {
							isExclude = true;
							break;
						}
					}
					if (!isExclude) {
						authhrefs.add(href);
					}
					break;
				}
			}
		}
		return authhrefs;
	}

	public static Set<String> findHref(String html) {
		Set<String> hrefs = new HashSet<String>();
		if (html == null) {
			return hrefs;
		}
		Pattern pattern = Pattern.compile("\"http:.*?\"");
		Matcher matcher = pattern.matcher(html);
		while (matcher.find()) {
			String s = matcher.group();
			if (s.indexOf(".js\"") > -1 || s.indexOf(".css\"") > -1
					|| s.indexOf(".ico\"") > -1 || s.indexOf(".jpg\"") > -1
					|| s.indexOf(".png\"") > -1) {
				continue;
			}
			s = s.replaceAll("\"", "");
			hrefs.add(s);
		}
		return hrefs;
	}

	@Override
	public void run() {
		String url = "http://movie.douban.com/subject/3014952/";
		String html = fetchHtml(url);
		Set<String> urls = findHref(html);
		urls = authHref(urls);
		queue.addAll(urls);
		while (true) {
			String u = queue.poll();
			
			if(u==null){
				continue;
			}
			html = fetchHtml(u);
			urls = findHref(html);
			urls = authHref(urls);
			queue.addAll(urls);
		}
	}
}
