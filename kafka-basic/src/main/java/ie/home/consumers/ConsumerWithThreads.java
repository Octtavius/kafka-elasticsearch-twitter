package ie.home.consumers;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import home.ie.KafkaManager;

public class ConsumerWithThreads {

	public static void main(String... args) {
		final Logger log = LoggerFactory.getLogger(ConsumerWithThreads.class);
		String groupId = "my-group";
		String topic = "first_topic";
		KafkaManager kafkaManager = new KafkaManager();
		CountDownLatch latch = new CountDownLatch(1);

		// create consumerrunnable object
		Runnable myConsumerThread = new ConsumerThread(latch, topic, groupId, kafkaManager);

		// start the thread
		Thread myThread = new Thread(myConsumerThread);
		myThread.start();

		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.info("Caught shutdown hook");
			((ConsumerThread) myConsumerThread).shutdown();
		}));
		
		log.info("hello");
		try {
			latch.await();
		} catch (InterruptedException e) {
			log.info("app got interruppet");
		} finally {
			log.info("app is closing");
		}

		log.info("ggiiig");
	}
//	KafkaManager kafkaManager;
//	private CountDownLatch latch;
//	KafkaConsumer<String, String> consumer;
//
//	public ConsumerWithThreads(CountDownLatch latch, String topic, String groupId) {
//		this.kafkaManager = new KafkaManager();
//		this.latch = latch;
//		consumer = new KafkaConsumer<>(kafkaManager.getConsumerProperty(groupId, topic));
//		consumer.subscribe(Collections.singleton(topic));
//	}
//
//	@Override
//	public void run() {
//		try {
//			// poll for new data
//			while (true) {
//				// Duration means to tell the consumer the timeout
//				// consumer will read each partition in order and read all values from each
//				// partition
//				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//
//				for (ConsumerRecord<String, String> record : records) {
//					log.info("Key: " + record.key() + ", Value: " + record.value());
//					log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
//				}
//			}
//		} catch (WakeupException e) {
//			log.info("Received shutdown signal!");
//		} finally {
//			consumer.close();
//			// tell our main code we are done with the consumer
//			latch.countDown();
//		}
//
//	}
//
//	public void shutdown() {
//		// the wakeup() is a special method to interrupt consumer.poll()
//		// it will throw the WakeUpException() exception
//		consumer.wakeup();
//	}

}
