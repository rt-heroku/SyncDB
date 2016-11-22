package com.heroku.syncdbs;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

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

			Main main = new Main();
			main.connectBothDBs();
			Map<String, Integer> tables = main.getTablesAndCount();

			for (String table : tables.keySet()) {
				int count = tables.get(table).intValue();
				JSONObject obj = new JSONObject();

				int chunk = 100000;
				int job = 0;
				int jobChunk = count;
				int offset = 0;

				main.recreateTable(table);

				while ( jobChunk > 0){
					
					int limit = jobChunk;
					if ((jobChunk - chunk) > 0) 
						limit = chunk;
					jobChunk = jobChunk - chunk;
					job++;

					obj.put("table", table);
					obj.put("count", count);
					obj.put("offset", offset);
					obj.put("limit", limit);
					obj.put("job", job);
					
//					channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN,
//							obj.toJSONString().getBytes("UTF-8"));

					System.out.println("MANUALLY Publishing job number[" + job + "] for TABLE[" + table + "] with total " + count + 
							" rows - OFFSET: " + offset + " - LIMIT: " + limit);
//					System.out.println(obj.toJSONString());

					offset = offset + chunk;
				}
				
			}
			
			main.closeBothConnections();
			connection.close();

		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
	}

}
