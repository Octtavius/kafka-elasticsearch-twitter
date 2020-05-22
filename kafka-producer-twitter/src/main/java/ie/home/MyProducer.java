package ie.home;

import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;

public class MyProducer extends KafkaProducer<String, String>{

	public MyProducer(Map<String, Object> configs) {
		super(configs);
		
	}

}