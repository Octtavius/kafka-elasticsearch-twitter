package ie.home.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import home.ie.KafkaManager;

/**
 * Hello world!
 *
 */
public class ProducerDemoWithKeys {
	static Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
	public static void main(String[] args) {

		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaManager.bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producer
		// because we produce string, values of kafkaproucer will be string string
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		String topic = "first_topic";
		
		for (int i = 0; i < args.length; i++) {
			String value = "hello world " + i;
			String key = "id_" + i;
			// create a producerRecord
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
			
			// send data - asynchronous
			producer.send(record, new Callback() {
				
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// executes every time a record is successfully sent or an exception is thrown
					
					if (exception == null) {
						// the record was successfully sent
						log.info("Received new metadata. \n" + 
								"Topic: " + metadata.topic() + 
								"\nPartition: " + metadata.partition() + 
								"\nOffset: " + metadata.offset() + 
								"\nTimestamp: "+ metadata.timestamp());
					} else {
						log.error("Error while writing" + exception);
					}
				}
			});
		}

		// flush
		producer.flush();

		// flush and close producer
		producer.close();
	}
}
