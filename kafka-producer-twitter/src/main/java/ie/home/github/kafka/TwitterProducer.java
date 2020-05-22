package ie.home.github.kafka;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.core.Client;

import ie.home.KafkaManager;
import ie.home.TwitterClientFactory;

public class TwitterProducer {
	static Logger log = LoggerFactory.getLogger(TwitterProducer.class);
	
	KafkaManager kafkaManager = new KafkaManager();
	TwitterClientFactory twitterClientFactory = new TwitterClientFactory();
	
	public TwitterProducer() {
	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}

	public void run() {
		log.info("Setup Application");
		/**
		 * This client basically will put the messages in this BlockingQueue
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		
		// create a twitter client
		List<String> terms = Lists.newArrayList("blockchain", "sport");
//		List<String> terms = Lists.newArrayList("blockchain", "api", "twitter", "politics", "sport");
		Client client = twitterClientFactory.createTwitterClient(msgQueue, terms);

		// Attempts to establish a connection.
		client.connect();
		
		
		// create a kafka producer
		KafkaProducer<String, String> producer = kafkaManager.createSafeProducer();
		
		//add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.info("Stopping application....");
			log.info("Shutting down client form twitter");
			client.stop();
			log.info("closing producer");
			producer.close();
		}));
		
		// loop to send tweets to kafka
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
		  try {
			msg = msgQueue.poll(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			client.stop();
		}

		  if (msg != null) {
//			  log.info(msg);
			  ProducerRecord<String, String> record = new ProducerRecord<String, String>("twitter_tweets", null, msg);
			  
			  // add a callback to catch errors
			  producer.send(record, new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception != null) {
						log.error("Something bad happened");
					}
				}
			});
		  }
		}
		
		log.info("End of application");
	}
}
