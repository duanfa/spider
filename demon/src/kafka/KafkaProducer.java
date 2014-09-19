package kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
	public static void main(String[] args) {
		Properties props = new Properties();
		// kafka.serializer.Encoder<T>
		props.put("zk.connect", "master:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "master:9092");
		props.put("request.required.acks", "1");
		// props.put("partitioner.class", "com.xq.SimplePartitioner");
		ProducerConfig config = new ProducerConfig(props);
		final Producer<String, String> producer = new Producer<String, String>(config);
		String ip = "master";
		SimpleDateFormat s = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss Z]");
		String topic_name = "sapi_sina_statuses";
		
		String msg = "{\"created_at\":\"Tue Sep 02 11:38:04 +0800 2014\",\"id\":3750318556355870,\"mid\":\"3750318556355870\",\"idstr\":\"3750318556355870\",\"text\":\"回复@馬克思和蠢得死:”9“~“2”~“5”三个数字任选其一即可哦 //@馬克思和蠢得死:请问是925都要有吗？\",\"source\":\"<a href=http://weibo.com/ rel=nofollow>微博 weibo.com<a>\",\"favorited\":false,\"truncated\":false,\"in_reply_to_status_id\":\"\",\"in_reply_to_user_id\":\"\",\"in_reply_to_screen_name\":\"\",\"pic_urls\":[],\"geo\":null,\"user\":{\"id\":2489283643,\"idstr\":\"2489283643\",\"screen_name\":\"王府井网上商城\",\"name\":\"王府井网上商城\",\"province\":\"11\",\"city\":\"1\",\"location\":\"北京 东城区\",\"description\":\"\",\"url\":\"\",\"profile_image_url\":\"http://tp4.sinaimg.cn/2489283643/50/5624774524/0\",\"cover_image\":\"http://ww1.sinaimg.cn/crop.0.0.980.300/945f743bgw1eel79ng6xcj20r808cdgl.jpg\",\"profile_url\":\"wangfujingcom\",\"domain\":\"wangfujingcom\",\"weihao\":\"\",\"gender\":\"f\",\"followers_count\":272177,\"friends_count\":438,\"pagefriends_count\":0,\"statuses_count\":3443,\"favourites_count\":11,\"created_at\":\"Thu Nov 24 09:23:06 +0800 2011\",\"following\":false,\"allow_all_act_msg\":false,\"geo_enabled\":true,\"verified\":true,\"verified_type\":2,\"remark\":\"\",\"ptype\":0,\"allow_all_comment\":true,\"avatar_large\":\"http://tp4.sinaimg.cn/2489283643/180/5624774524/0\",\"avatar_hd\":\"http://tp4.sinaimg.cn/2489283643/180/5624774524/0\",\"verified_reason\":\"北京王府井百货集团北京网尚电子商务有限责任公司\",\"verified_trade\":\"\",\"verified_reason_url\":\"\",\"verified_source\":\"\",\"verified_source_url\":\"\",\"verified_state\":0,\"follow_me\":false,\"online_status\":0,\"bi_followers_count\":105,\"lang\":\"zh-cn\",\"star\":0,\"mbtype\":0,\"mbrank\":0,\"block_word\":0,\"block_app\":0,\"credit_score\":0},\"retweeted_status\":{\"created_at\":\"Tue Sep 02 11:33:11 +0800 2014\",\"id\":3750317327906917,\"mid\":\"3750317327906917\",\"idstr\":\"3750317327906917\",\"text\":\"#925周年庆#【2014年9月25日王府井百货集团59岁生日小井开了生日趴】 9.2-9.16活动期间，井粉可将自己与“9、2、5”相关的照片，例如“二”字剪刀手照片等，发布微博并艾特三名好友，说出对王府井集团的生日祝福，活动结束将有15名幸运小主有机会机会获得小井送出的精美大福袋噢。http://t.cn/8kdKT5c\",\"source\":\"<a href=http://weibo.com/ rel=nofollow>微博 weibo.com<a>\",\"favorited\":false,\"truncated\":false,\"in_reply_to_status_id\":\"\",\"in_reply_to_user_id\":\"\",\"in_reply_to_screen_name\":\"\",\"pic_urls\":[{\"thumbnail_pic\":\"http://ww1.sinaimg.cn/thumbnail/945f743bjw1ejxyumvdtoj20ri0rswl4.jpg\"}],\"thumbnail_pic\":\"http://ww1.sinaimg.cn/thumbnail/945f743bjw1ejxyumvdtoj20ri0rswl4.jpg\",\"bmiddle_pic\":\"http://ww1.sinaimg.cn/bmiddle/945f743bjw1ejxyumvdtoj20ri0rswl4.jpg\",\"original_pic\":\"http://ww1.sinaimg.cn/large/945f743bjw1ejxyumvdtoj20ri0rswl4.jpg\",\"geo\":null,\"user\":{\"id\":2489283643,\"idstr\":\"2489283643\",\"screen_name\":\"王府井网上商城\",\"name\":\"王府井网上商城\",\"province\":\"11\",\"city\":\"1\",\"location\":\"北京 东城区\",\"description\":\"\",\"url\":\"\",\"profile_image_url\":\"http://tp4.sinaimg.cn/2489283643/50/5624774524/0\",\"cover_image\":\"http://ww1.sinaimg.cn/crop.0.0.980.300/945f743bgw1eel79ng6xcj20r808cdgl.jpg\",\"profile_url\":\"wangfujingcom\",\"domain\":\"wangfujingcom\",\"weihao\":\"\",\"gender\":\"f\",\"followers_count\":272177,\"friends_count\":438,\"pagefriends_count\":0,\"statuses_count\":3443,\"favourites_count\":11,\"created_at\":\"Thu Nov 24 09:23:06 +0800 2011\",\"following\":false,\"allow_all_act_msg\":false,\"geo_enabled\":true,\"verified\":true,\"verified_type\":2,\"remark\":\"\",\"ptype\":0,\"allow_all_comment\":true,\"avatar_large\":\"http://tp4.sinaimg.cn/2489283643/180/5624774524/0\",\"avatar_hd\":\"http://tp4.sinaimg.cn/2489283643/180/5624774524/0\",\"verified_reason\":\"北京王府井百货集团北京网尚电子商务有限责任公司\",\"verified_trade\":\"\",\"verified_reason_url\":\"\",\"verified_source\":\"\",\"verified_source_url\":\"\",\"verified_state\":0,\"follow_me\":false,\"online_status\":0,\"bi_followers_count\":105,\"lang\":\"zh-cn\",\"star\":0,\"mbtype\":0,\"mbrank\":0,\"block_word\":0,\"block_app\":0,\"credit_score\":0},\"reposts_count\":4,\"comments_count\":9,\"attitudes_count\":0,\"mlevel\":0,\"visible\":{\"type\":0,\"list_id\":0},\"darwin_tags\":[]},\"reposts_count\":0,\"comments_count\":0,\"attitudes_count\":0,\"mlevel\":0,\"visible\":{\"type\":0,\"list_id\":0},\"darwin_tags\":[]}";
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic_name, ip, msg);
		producer.send(data);
		
		topic_name = "sapi_sina_userinfo";
		
		//msg = "{\"id\":1762883264,\"idstr\":\"1762883264\",\"screen_name\":\"jazzzzzzt\",\"name\":\"jazzzzzzt\",\"province\":\"35\",\"city\":\"2\",\"location\":\"福建 厦门\",\"description\":\"不好玩\",\"url\":\"http://kannimabi.com\",\"profile_image_url\":\"http://tp1.sinaimg.cn/1762883264/50/5601128638/1\",\"profile_url\":\"u/1762883264\",\"domain\":\"\",\"weihao\":\"\",\"gender\":\"m\",\"followers_count\":235,\"friends_count\":430,\"statuses_count\":627,\"favourites_count\":27,\"created_at\":\"Wed Jun 16 20:54:54 +0800 2010\",\"following\":true,\"allow_all_act_msg\":false,\"geo_enabled\":true,\"verified\":false,\"verified_type\":-1,\"remark\":\"\",\"status\":{\"created_at\":\"Thu Aug 28 18:51:13 +0800 2014\",\"id\":3748615627915556,\"mid\":\"3748615627915556\",\"idstr\":\"3748615627915556\",\"text\":\"求中\",\"source\":\"ssssssssssssssssssss\",\"favorited\":false,\"truncated\":false,\"in_reply_to_status_id\":\"\",\"in_reply_to_user_id\":\"\",\"in_reply_to_screen_name\":\"\",\"pic_urls\":[],\"geo\":null,\"reposts_count\":0,\"comments_count\":0,\"attitudes_count\":0,\"mlevel\":0,\"visible\":{\"type\":0,\"list_id\":0},\"darwin_tags\":[]},\"ptype\":0,\"allow_all_comment\":true,\"avatar_large\":\"http://tp1.sinaimg.cn/1762883264/180/5601128638/1\",\"avatar_hd\":\"http://tp1.sinaimg.cn/1762883264/180/5601128638/1\",\"verified_reason\":\"\",\"verified_trade\":\"\",\"verified_reason_url\":\"\",\"verified_source\":\"\",\"verified_source_url\":\"\",\"follow_me\":false,\"online_status\":0,\"bi_followers_count\":46,\"lang\":\"zh-cn\",\"star\":0,\"mbtype\":0,\"mbrank\":0,\"block_word\":0,\"block_app\":0,\"climb_type\":\"kafka\",\"climb_time\":1409710953}";
		data = new KeyedMessage<String, String>(topic_name, ip, msg);
		producer.send(data);
		
		producer.close();
	}
}