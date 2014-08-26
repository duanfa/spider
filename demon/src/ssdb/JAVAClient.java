package ssdb;

import com.udpwork.ssdb.SSDB;

public class JAVAClient {
	public static void main(String[] args) throws Exception {
		String uid = "";
		SSDB redis1 = new SSDB("192.168.0.28", 8891);
		SSDB redis2 = new SSDB("192.168.0.29", 8891);
		String fans =  hgetV(uid, redis1,"followers_count");
		String favs = hgetV(uid, redis1,"favourites_count");
		String screen_name =  hgetV(uid, redis1,"screen_name");
		String avg_forward_status =  hgetV(uid, redis2,"avg_forward_status");
		String avg_commemts_status =  hgetV(uid, redis2,"avg_commemts_status");
		String navy_base_data =  hgetV(uid, redis2,"shuidu");
		String avg_status_num =  hgetV(uid, redis1,"");
		String interact_user =  hgetV(uid, redis1,"");
		String real_degree =  hgetV(uid, redis2,"real_degree");
		String user_pr =  hgetV(uid, redis2,"org_pr");
		String dispose_pr =  hgetV(uid, redis2,"dispose_pr");
		String location =  hgetV(uid, redis1,"");
		String description =  hgetV(uid, redis1,"");
		String gender =  hgetV(uid, redis1,"");
		String friends_count =  hgetV(uid, redis1,"");
		String statuses_count =  hgetV(uid, redis1,"");
		String verified =  hgetV(uid, redis1,"");
		String verified_reason =  hgetV(uid, redis1,"");
		String province =  hgetV(uid, redis1,"");
		String city =  hgetV(uid, redis1,"");
		String liveness =  hgetV(uid, redis2,"liveness");
		String navy =  hgetV(uid, redis2,"shuidu");
		String user_define_tag =  hgetV(uid, redis1,"");
		
		String head = "uid,fans,favs,screen_name,avg_forward_status,avg_commemts_status,navy_base_data,avg_status_num,interact_user,real_degree,user_pr,dispose_pr,location,description,gender,friends_count,statuses_count,verified,verified_reason,province,city,liveness,navy,user_define_tag";
	}

	private static String hgetV(String uid, SSDB ssdb,String key){
		try {
			return new String(ssdb.hget(uid,key));
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
	}
}
