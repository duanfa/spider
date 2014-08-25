package kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import avro.User;

public class KafkaAvroConsumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("zookeeper.connect", "master:2181");
		props.put("zookeeper.connection.timeout.ms", "10000");
		props.put("group.id", "test_group");
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.SCHEMA$);
		
		
		// Create the connection to the cluster
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		kafka.javaapi.consumer.ConsumerConnector connector = Consumer.createJavaConsumerConnector(consumerConfig);
		Map<String, Integer> topics = new HashMap<String, Integer>();
		topics.put("topic_4", 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = connector.createMessageStreams(topics);
		// ExecutorService threadPool = Executors.newFixedThreadPool(2);
		List<KafkaStream<byte[], byte[]>> streams = topicMessageStreams.get("topic_4");
		int i = 0;
		User u = null;
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (it.hasNext()){
				byte[] buf = it.next().message();
				Decoder d  = DecoderFactory.get().binaryDecoder(buf, null);
				try {
					u = userDatumReader.read(null, d);
				} catch (IOException e) {
					e.printStackTrace();
				}
				DataFileReader<User> dataFileReader = null;
				
				System.out.println(u.getName());
			}
		}
	}
}