package coockie;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

public class robot {
	public static void main(String[] args) throws IOException {
		URL url = new URL("http://movie.douban.com/");
		HttpURLConnectionWrapper wraper = new HttpURLConnectionWrapper(url);
		InputStream is = wraper.getInputStream();
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String line = "";
		while((line=br.readLine())!=null){
			System.out.println(line);
		}
		br.close();
	}
}
