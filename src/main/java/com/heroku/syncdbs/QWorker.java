package com.heroku.syncdbs;

import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class QWorker {
	public static void main(String[] args) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri(System.getenv("CLOUDAMQP_URL"));

		Map<String, Object> params = new HashMap<String, Object>();
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		String queueName = "" + System.getenv("QUEUE_NAME");
		params.put("x-ha-policy", "all");
		channel.queueDeclare(queueName, true, false, false, params);
		
		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.basicConsume(queueName, false, consumer);

		while (true) {
			QueueingConsumer.Delivery delivery = consumer.nextDelivery();
			if (delivery != null) {
				String msg = new String(delivery.getBody(), "UTF-8");

				System.out.println("QWorker ---- Message Received: " + msg);
				channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			}
		}

	}
}
