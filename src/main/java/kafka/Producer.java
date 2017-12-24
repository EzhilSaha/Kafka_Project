package kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import probuff.EmployeeOuterClass.Employee;

//Handler Class to handle server request
public class Producer {
	
	public static void producer(String topic,probuff.EmployeeOuterClass.Employee employee) {

		if (topic == null) {
			System.err.println("No topic Found or Topic is null");
			System.exit(-1);
		}
		
		String topicName = topic;
		Properties configProperties = new Properties();
		KafkaProducer<String, Employee> producer = null;
		
		try
		{
			//Configure the Producer
			
			configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
			
			configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
			
			ProducerRecord<String, Employee> rec = null;
			
			configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"kafka.EmployeeSerializer");
			
			producer = new KafkaProducer<String, Employee>(configProperties);

			rec = new ProducerRecord<String, Employee>(topicName, employee);
						
			producer.send(rec);
		
		}
		catch(Exception e)
		{
		
			System.out.println(e);
			System.out.println(e.getStackTrace());
			
		}
		
		producer.close();
		
		System.out.println("Object Successfully pushed to Kafka Queue");
	}

}