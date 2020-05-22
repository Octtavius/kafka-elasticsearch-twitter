package ie.home.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import home.ie.KafkaManager;

public class ConsumerThread implements Runnable {
	static Logger log = LoggerFactory.getLogger(ConsumerThread.class);
	KafkaManager kafkaManager;
	private CountDownLatch latch;
	KafkaConsumer<String, String> consumer;

	public ConsumerThread(CountDownLatch latch, String topic, String groupId, KafkaManager kafkaManager) {
		this.kafkaManager = new KafkaManager();
		this.latch = latch;
		consumer = new KafkaConsumer<>(kafkaManager.getConsumerProperty(groupId, topic));
		consumer.subscribe(Collections.singleton(topic));
	}

	@Override
	public void run() {
		try {
			// poll for new data
			while (true) {
				// Duration means to tell the consumer the timeout
				// consumer will read each partition in order and read all values from each
				// partition
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : records) {
					log.info("Key: " + record.key() + ", Value: " + record.value());
					log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
				}
			}
		} catch (WakeupException e) {
			log.info("Received shutdown signal!");
		} finally {
			consumer.close();
			// tell our main code we are done with the consumer
			latch.countDown();
		}

	}

	public void shutdown() {
		// the wakeup() is a special method to interrupt consumer.poll()
		// it will throw the WakeUpException() exception
		consumer.wakeup();
	}
}
