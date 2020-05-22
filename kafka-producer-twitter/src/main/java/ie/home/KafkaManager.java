package ie.home;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaManager {

	Properties producerProperties;
	Properties consumerProperties;

	public static final String bootstrapServer = "127.0.0.1:9092";

	public KafkaManager() {
		init();
	}

	private void init() {

	}

	public KafkaProducer<String, String> createProducer() {
		return new KafkaProducer<String, String>(getProducerProperty());
	}
	

	public Properties getConsumerProperty(String groupId, String topic) {
		// create producer properties
		consumerProperties = new Properties();
		consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaManager.bootstrapServer);
		consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

		// earliest: means that you want to read from the very very beginning of your
		// topic,
		// latest: is going to be when you read from only the new messages onwards and
		// none: will throw an error if there is no offsets being saved
		consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // values can be
		return consumerProperties;
																						// earliest/latest/none
	}

	private Properties getProducerProperty() {
		producerProperties = new Properties();
		// create producer properties
		producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaManager.bootstrapServer);
		producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return producerProperties;
	}
	public KafkaProducer<String, String> createSafeProducer() {
		return new KafkaProducer<String, String>(getSafeProducerProperty());
	}

	private Properties getSafeProducerProperty() {
		Properties safeProperty = new Properties();
		// create producer properties
		safeProperty.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaManager.bootstrapServer);
		safeProperty.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		safeProperty.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		safeProperty.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

		// We don't need to set those, but it's always good to set them explicitly just to make sure that people don't guess and understand how things work.
		safeProperty.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		safeProperty.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		
		// if kafka > 1.1, then set connection to 5, otherwise set it to 1 
		safeProperty.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		
		// high throughput producer (at the expense of a bit of latency and CPU usage)
		safeProperty.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		// it is ok to get my tweets 20 ms delayed, we will get a lot of tweets at a time
		safeProperty.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		safeProperty.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32KB batch size
		
		
		return safeProperty;
	}
}
