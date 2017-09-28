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

		props.put("bootstrap.servers", "192.168.3.198:9092");
		System.out.println("this is the group part test 1");
		// 消费者的组id
		props.put("group.id", "GroupA");// 这里是GroupA或者GroupB
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");

		// 从poll(拉)的会话处理时长
		props.put("session.timeout.ms", "30000");
		props.put("max.poll.records", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
		// 订阅主题列表topic
		consumer.subscribe(Arrays.asList("foo5"));
		//
		try
		{
			int i =0;
			while (true)
			{
				System.out.println("consumer "+(i++)+"... ");
				ConsumerRecords<String, String> records = consumer.poll(100);
				if(records.count()==0)
				{
					Thread.sleep(50);
				}else
				{
					System.out.println("poll "+records.count());
					for (ConsumerRecord<String, String> record : records)
					// 　正常这里应该使用线程池处理，不应该在这里处理
					System.out.printf("topic = %s, partition = %d,offset = %d, key = %s, value = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value() + "\n");
					
				}
				Thread.sleep(500);
			}
		} catch (Exception e)
		{
			e.printStackTrace();
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
