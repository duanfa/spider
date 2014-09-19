package kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerConnector;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class KafkaConsumer {
	public static void main(String[] args) {
		// specify some consumer properties
		Properties props = new Properties();
		props.put("zookeeper.connect", "master:2181");
		props.put("zookeeper.connection.timeout.ms", "10000");
		props.put("group.id", "test_group");
		// Create the connection to the cluster
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		kafka.javaapi.consumer.ConsumerConnector connector = Consumer.createJavaConsumerConnector(consumerConfig);
		Map<String, Integer> topics = new HashMap<String, Integer>();
		topics.put("sapi_sina_statuses", 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = connector.createMessageStreams(topics);
		// ExecutorService threadPool = Executors.newFixedThreadPool(2);
		List<KafkaStream<byte[], byte[]>> streams = topicMessageStreams.get("sapi_sina_statuses");
		int i = 0;
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (it.hasNext())
				System.out.println(new String(it.next().message()) + "," + i++);
		}
	}
}