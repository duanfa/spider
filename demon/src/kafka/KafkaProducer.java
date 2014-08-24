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
		String topic_name = "topic_3";
		
		String msg = "{\"name\":\"xiaomeng\",\"timestamp\":\"" + s.format(new Date()) + "\"}";
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic_name, ip, msg);
		producer.send(data);
		
		msg = "{\"name\":\"xiaocao\",\"timestamp\":\"" + s.format(new Date()) + "\"}";
		data = new KeyedMessage<String, String>(topic_name, ip, msg);
		producer.send(data);
		
		msg = "{\"name\":\"xiaozhu\",\"timestamp\":\"" + s.format(new Date()) + "\"}";
		data = new KeyedMessage<String, String>(topic_name, ip, msg);
		producer.send(data);
		
		msg = "{\"name\":\"xiaopan\",\"timestamp\":\"" + s.format(new Date()) + "\"}";
		data = new KeyedMessage<String, String>(topic_name, ip, msg);
		producer.send(data);
		
		producer.close();
	}
}