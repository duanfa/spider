package ssdb;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;

import com.udpwork.ssdb.SSDB;

public class JAVAClient {
	
	private static ByteArrayOutputStream bos = new ByteArrayOutputStream(512);
	private static DataOutputStream dos = new DataOutputStream(bos);
	static byte[] buf ;
	public static void main(String[] args) throws Exception {
		SSDB ssdb = new SSDB(args[1], Integer.parseInt(args[2]));
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(args[0])));
		String line = "";
		boolean print = false;
		if(args.length>4&&"true".equals(args[4])){
			print = true;
		}
		while((line= br.readLine())!=null){
			try {
				JSONObject jsonObject = JSONObject.fromObject(line);
				Long sid = jsonObject.getLong("id");
				String fans =  hgetV(sid+"", ssdb, args[3]);
				if(StringUtils.isNotBlank(fans)){
					if(print){
						System.out.println(sid+"'s "+ args[3] +" is :"+fans);
					}
				}else{
					jsonObject.accumulate("error", sid+"'s "+ args[3] +" is null");
					System.out.println(jsonObject.toString());
				}
			} catch (Exception e) {
				System.out.println(line);
				e.printStackTrace();
			}
		}
		br.close();
		dos.close();
		
	}

	private static String hgetV(String uid, SSDB ssdb,String key){
		try {
			return new String(ssdb.hget(uid,key));
		} catch (Exception e) {
			//e.printStackTrace();
			return "";
		}
		
	}
}
