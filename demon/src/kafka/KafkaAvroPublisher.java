package kafka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.IOUtils;

import avro.User;

public class KafkaAvroPublisher {

	private final Producer<String, byte[]> kafkaProducer;
	private static final SpecificDatumWriter<User> avroEventWriter = new SpecificDatumWriter<User>(User.SCHEMA$);

	public KafkaAvroPublisher() {
		Properties props = new Properties();
		props.put("zk.connect", "master:2181");
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");
		// props.put("serializer.class", "org.apache.avro.io.BinaryEncoder");
		props.put("metadata.broker.list", "master:9092");
		kafkaProducer = new Producer<String, byte[]>(new ProducerConfig(props));
	}

	public void publish(User user) {
		try {

			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(stream, null);
			avroEventWriter.write(user, binaryEncoder);
			binaryEncoder.flush();
			IOUtils.closeQuietly(stream);

			GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(User.SCHEMA$);
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			try {
				Encoder e = EncoderFactory.get().binaryEncoder(os, null);
				writer.write(user, e);
				e.flush();
			} finally {
				os.close();
			}

			Message m = new Message(stream.toByteArray());
			Message m2 = new Message(os.toByteArray());
			String topic_name = "topic_4";
			KeyedMessage<String, Message> data = new KeyedMessage<String, Message>(topic_name, m);
			KeyedMessage<String, byte[]> data2 = new KeyedMessage<String, byte[]>(topic_name, os.toByteArray());
			//while(true){
				kafkaProducer.send(data2);
			//}
			kafkaProducer.close();
		} catch (IOException e) {
			throw new RuntimeException("Avro serialization failure", e);
		}
	}

	public static void main(String[] args) {
		KafkaAvroPublisher publisher = new KafkaAvroPublisher();
		User user = new User();
		user.setName("zhangsan");
		user.setFavoriteNumber(5);
		user.setFavoriteColor("yellow");
		publisher.publish(user);
	}
}