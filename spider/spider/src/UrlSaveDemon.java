

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class UrlSaveDemon implements Runnable {
	
	static Set<String> usedUrls = new HashSet<String>();
	static Set<String> illegalUrls = new HashSet<String>();
	static{
		usedUrls = Collections.synchronizedSet(usedUrls);
		try {
			FileInputStream fis = new FileInputStream(Constant.usedUrlFilePath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String url = "";
			while((url=br.readLine())!=null){
				usedUrls.add(url);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		FileOutputStream usedFos = null;
		BufferedWriter usedBw = null;
		FileOutputStream illegalFos = null;
		BufferedWriter illegalBw = null;
		try {
			usedFos = new FileOutputStream(Constant.usedUrlFilePath,true);
			usedBw = new BufferedWriter(new OutputStreamWriter(usedFos));
			illegalFos = new FileOutputStream(Constant.usedUrlFilePath,true);
			illegalBw = new BufferedWriter(new OutputStreamWriter(illegalFos));
		} catch (FileNotFoundException e3) {
			e3.printStackTrace();
		}
		while(true){
			try {
				Thread.currentThread();
				Thread.sleep(100000);
				Set<String> usedUrls_tmp = usedUrls;
				usedUrls = new HashSet<String>();
				usedUrls = Collections.synchronizedSet(usedUrls);
				Set<String> illegalUrls_tmp = illegalUrls;
				illegalUrls = new HashSet<String>();
				illegalUrls = Collections.synchronizedSet(illegalUrls);
				for(String url:usedUrls_tmp){
					usedBw.newLine();
					usedBw.write(url);
				}
				for(String url:illegalUrls_tmp){
					illegalBw.newLine();
					illegalBw.write(url);
				}
			} catch (Exception e) {
				e.printStackTrace();
				try {
					usedBw.close();
					illegalBw.close();
					usedFos = new FileOutputStream(Constant.usedUrlFilePath,true);
					usedBw = new BufferedWriter(new OutputStreamWriter(usedFos));
					illegalFos = new FileOutputStream(Constant.usedUrlFilePath,true);
					illegalBw = new BufferedWriter(new OutputStreamWriter(illegalFos));
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			}
		}
	}

}
