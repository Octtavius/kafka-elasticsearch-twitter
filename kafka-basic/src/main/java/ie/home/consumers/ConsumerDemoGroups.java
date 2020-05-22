package ie.home.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import home.ie.KafkaManager;

public class ConsumerDemoGroups {
	
	static Logger log = LoggerFactory.getLogger(ConsumerDemoGroups.class);

	public static void main(String... args) {
		String groupId = "my-fourth-application";
		String topic = "first_topic";
		
		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaManager.bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		
		// earliest:  means that you want to read from the very very beginning of your topic, 
		// latest:    is going to be when you read from only the new messages onwards and 
		// none:      will throw an error if there is no offsets being saved
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // values can be earliest/latest/none
		
		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		
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
