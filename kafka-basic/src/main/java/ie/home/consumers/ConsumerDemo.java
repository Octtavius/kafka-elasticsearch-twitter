package ie.home.consumers;

import java.time.Duration;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import home.ie.KafkaManager;

public class ConsumerDemo {
	
	static Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

	public static void main(String... args) {
		String groupId = "my-fourth-application";
		String topic = "first_topic";
		KafkaManager kafkaManager = new KafkaManager();
		
		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaManager.getConsumerProperty(groupId, topic));
		
		// subscribe consumer to our topic(s)
		consumer.subscribe(Collections.singleton(topic));
		//example of how to subscribe to multiple topics
		// consumer.subscribe(Arrays.asList("first_topic", "second_topic"));		
		
		// poll for new data
		while (true) {
			// Duration means to tell the consumer the timeout
			// consumer will read each partition in order and read all values from each partition
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for (ConsumerRecord<String, String> record: records) {
				log.info("Key: " + record.key() + ", Value: " + record.value());
				log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
			}
		}
		
	}
}
