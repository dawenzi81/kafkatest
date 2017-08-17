/**
 * 
 */
package com.kgzt.kfk;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author Guo
 *
 */
public class MyConsumer
{

	/**
	 * 
	 */
	public MyConsumer()
	{
		// TODO Auto-generated constructor stub
	}

	public void consume()
	{
		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		System.out.println("this is the group part test 1");
		// 消费者的组id
		props.put("group.id", "GroupA");// 这里是GroupA或者GroupB

		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");

		// 从poll(拉)的回话处理时长
		props.put("session.timeout.ms", "30000");
		// props.put("max.poll.records", "100");

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		// 订阅主题列表topic
		consumer.subscribe(Arrays.asList("foo","fox","foo2","foo4"));

		while (true)
		{
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				// 　正常这里应该使用线程池处理，不应该在这里处理
				System.out.printf("topic = %s, partition = %d,offset = %d, key = %s, value = %s", record.topic(),record.partition(),record.offset(), record.key(), record.value() + "\n");

		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		// TODO Auto-generated method stub
new MyConsumer().consume();

	}

}