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

public class QWorker {

	private JSONParser parser = null;

	public QWorker() {
		parser = new JSONParser();
	}

	public static void main(String[] args) throws Exception {
		QWorker q = new QWorker();
		q.run();
	}

	public void run() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri(System.getenv("RABBITMQ_BIGWIG_URL"));

		Map<String, Object> params = new HashMap<String, Object>();
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		String queueName = "" + System.getenv("QUEUE_NAME");
		params.put("x-ha-policy", "all");
		channel.queueDeclare(queueName, true, false, false, params);
		channel.basicQos(1);

		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.basicConsume(queueName, false, consumer);

		Main main = new Main();
		main.connectBothDBs();
		String fromSchema = main.getFromSchema();
		String toSchema = main.getToSchema();
		try {
			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				if (delivery != null) {
					long t1 = System.currentTimeMillis();

					String msg = new String(delivery.getBody(), "UTF-8");
					JSONObject jobj = (JSONObject) parser.parse(msg);

					String jobid = (String) jobj.get("jobid");
					String table = (String) jobj.get("table");
					Integer offset = new Integer(jobj.get("offset").toString());
					Integer chunk = new Integer(jobj.get("chunk").toString());
					Integer jobnum = new Integer(jobj.get("jobnum").toString());
					Integer totalJobs = new Integer(jobj.get("totaljobs").toString());
					
					//log initial task
					// log milliseconds in 0
					String current = getCurrentTime();
					String logmsg = "QWorker Starting [" + current + "] Job ID[" + jobid + "] (" + jobnum + " of " + totalJobs + ") ---- Message Received: " + jobj.toJSONString();
					System.out.println(logmsg);
					main.copyTableChunk(fromSchema, toSchema, table, offset, chunk, jobnum, jobid);

					channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

					long t2 = System.currentTimeMillis();
					//log final task data with t2, 
					long seconds = (t2 - t1) /1000;
					logmsg = "QWorker ENDED     ["+ getCurrentTime() + "] Job ID [" + jobid + "] (" + jobnum + " of " + totalJobs + ") in [" + seconds + "] seconds for table [" + table + "] !";
					System.out.println(logmsg);
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
	
	
//	public boolean jobidExistsInTable(String jobid) throws Exception{
//		ResultSet rs = null;
//		boolean ret = false;
//		String sql = "SELECT jobid FROM syncdb.job_detail WHERE jobid='" + getJobid() + "' AND table='" + table + "' and " + column + "=" + value;
//		rs = getSource().prepareStatement(sql).executeQuery();
//		ret = rs.next();
//		rs.close();
//		return ret;
//	}
//
//	private void logTask(String table, int rows){
//		new Thread(() -> {
//			try {
//				String sql = "";
//				if (tasknumExistsInTable(table, "tasknum", getTaskNum())){
//					sql = "UPDATE syncdb.task SET index_loaded = " + rows +
//							" WHERE jobid='" + getJobid() + "' AND table='" + table + "'";
//				}else{
//					sql = "INSERT INTO syncdb.task (jobid, table, tasknum, index_loaded) VALUES('" + getJobid() + "','" + table + "'," + getTaskNum() + "," + rows + ")";
//				}
//				getSource().execute(sql);
//			} catch (Exception e) {
//				System.err.println("Error while logging table [" + table + "] load for job id [" + getJobid() + "]" + e.getMessage());
//			}
//		
//		}).start();
//	}

	
}
