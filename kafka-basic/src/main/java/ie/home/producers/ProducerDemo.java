package ie.home.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import home.ie.KafkaManager;

/**
 * Hello world!
 *
 */
public class ProducerDemo {
	static Logger log = LoggerFactory.getLogger(ProducerDemo.class);
	public static void main(String[] args) {

		KafkaManager kafkaManager = new KafkaManager(); 

		// create producer
		// because we produce string, values of kafkaproucer will be string string
		KafkaProducer<String, String> producer = kafkaManager.createProducer();

		// create a producerRecord
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World 223");

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

		// flush
		producer.flush();

		// flush and close producer
		producer.close();
	}
}
