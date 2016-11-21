package com.heroku.syncdbs;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class RunJob {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		try {

			ConnectionFactory factory = new ConnectionFactory();
			factory.setUri(System.getenv("CLOUDAMQP_URL"));
			
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();
			String queueName = "" + System.getenv("QUEUE_NAME");
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("x-ha-policy", "all");
			channel.queueDeclare(queueName, true, false, false, params);

			Map<String, Integer> tables = Main.getTablesAndCount();
			for (String table : tables.keySet()) {
				int count = tables.get(table).intValue();

				JSONObject obj = new JSONObject();

				obj.put("table", table);
				obj.put("count", count);

				channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN,
						obj.toJSONString().getBytes("UTF-8"));
				
				System.out.println("MANUALLY Publishing job for TABLE[" + table + "] with " + count + " rows... -- "
						+ obj.toJSONString());
			}

			connection.close();

		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}

	}

}
