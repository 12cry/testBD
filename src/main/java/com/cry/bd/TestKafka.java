
package com.cry.bd;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class TestKafka {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", TestSpark.IP+":9092");
		props.put("retries", 0);
		props.put("linger.ms", 1);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		props.put("partitioner.class", "com.goodix.kafka.MyPartition");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", "222", "bbb");
		producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null)
                    System.out.println("the producer has a error:" + e.getMessage());
                else {
                	System.out.println("The offset of the record we just sent is: " + metadata.offset());
                	System.out.println("The partition of the record we just sent is: " + metadata.partition());
                }
            }
        });
		producer.close();
	}
}