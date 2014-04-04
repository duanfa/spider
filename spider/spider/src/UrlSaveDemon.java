

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
	
	static Set<String> downloadUrls = new HashSet<String>();
	static Set<String> usedUrls = new HashSet<String>();
	static Set<String> illegalUrls = new HashSet<String>();
	static{
		downloadUrls = Collections.synchronizedSet(downloadUrls);
		try {
			FileInputStream fis = new FileInputStream(Constant.downloadUrlFilePath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String url = "";
			while((url=br.readLine())!=null){
				downloadUrls.add(url);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		FileOutputStream downloadFos = null;
		BufferedWriter downloadBw = null;
		FileOutputStream illegalFos = null;
		BufferedWriter illegalBw = null;
		try {
			downloadFos = new FileOutputStream(Constant.downloadUrlFilePath,true);
			downloadBw = new BufferedWriter(new OutputStreamWriter(downloadFos));
			illegalFos = new FileOutputStream(Constant.illegalUrlFilePath,true);
			illegalBw = new BufferedWriter(new OutputStreamWriter(illegalFos));
		} catch (Exception e3) {
			e3.printStackTrace();
		}
		while(true){
			try {
				Thread.currentThread().sleep(100000);
				Set<String> usedUrls_tmp = usedUrls;
				usedUrls = new HashSet<String>();
				usedUrls = Collections.synchronizedSet(usedUrls);
				Set<String> illegalUrls_tmp = illegalUrls;
				illegalUrls = new HashSet<String>();
				illegalUrls = Collections.synchronizedSet(illegalUrls);
				for(String url:usedUrls_tmp){
					downloadBw.newLine();
					downloadBw.write(url);
				}
				for(String url:illegalUrls_tmp){
					illegalBw.newLine();
					illegalBw.write(url);
				}
				downloadBw.flush();
				illegalBw.flush();
			} catch (Exception e) {
				e.printStackTrace();
				try {
					downloadBw.close();
					illegalBw.close();
					downloadFos = new FileOutputStream(Constant.downloadUrlFilePath,true);
					downloadBw = new BufferedWriter(new OutputStreamWriter(downloadFos));
					illegalFos = new FileOutputStream(Constant.illegalUrlFilePath,true);
					illegalBw = new BufferedWriter(new OutputStreamWriter(illegalFos));
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			}
		}
	}

}
