package com.heroku.syncdbs;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class LogWorker {

	private JSONParser parser = null;

	public LogWorker() {
		parser = new JSONParser();
	}

	public static void main(String[] args) throws Exception {
		LogWorker q = new LogWorker();
		q.run();
	}

	public void run() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri(System.getenv("RABBITMQ_BIGWIG_URL"));

		Map<String, Object> params = new HashMap<String, Object>();
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		String queueName = "" + System.getenv("LOG_QUEUE_NAME");
		params.put("x-ha-policy", "all");
		channel.queueDeclare(queueName, true, false, false, params);
		channel.basicQos(1);

		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.basicConsume(queueName, false, consumer);

		Main main = new Main();
		main.connectToSource();
		try {
			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				if (delivery != null) {
					String msg = new String(delivery.getBody(), "UTF-8");
					JSONObject jobj = (JSONObject) parser.parse(msg);

					System.out.println("DEBUG: Logging -- " + jobj.toJSONString());
					
					channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
				}
			}
		} catch (Exception e) {
			main.closeBothConnections();
			throw e;
		}
	}
	protected static String getCurrentTime() {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		return sdf.format(cal.getTime());
	}
}
