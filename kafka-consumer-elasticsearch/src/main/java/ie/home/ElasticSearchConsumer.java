package ie.home;

import java.io.IOException;
import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {
	static Logger log = LoggerFactory.getLogger(ElasticSearchConsumer.class);
	
	public static void main(String[] args) throws IOException, InterruptedException {
		// you will find this data in Access/Credentials in bonsai
		String hostname = "";
		String username = "";
		String password = "";
		
		KafkaManager kafkaManager = new KafkaManager();
		ElasticSearchManager esManager = new ElasticSearchManager(hostname, username, password);
		RestHighLevelClient client = esManager.createEsClient();
		
		KafkaConsumer<String, String> consumer = kafkaManager.createConsumer("twitter_tweets", "kafka-demo-elasticsearch");
		
		// poll for new data
		while (true) {
			// Duration means to tell the consumer the timeout
			// consumer will read each partition in order and read all values from each partition
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for (ConsumerRecord<String, String> record: records) {
				// where we insert data into elastic search
				String tweet = record.value();
				
				// make sure this index exist in ES
				IndexRequest indexRequest = new IndexRequest("twitter");
				indexRequest.source(tweet, XContentType.JSON);// we pass what we want to store and the type of the content
				
				IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
				String id = indexResponse.getId();
				log.info("id" + id);
				
				// this is to slow down the insertion of tweets by consumer into ES/ remove it later
				Thread.sleep(1000);
			}
		}
		
//		client.close();
	}
}
