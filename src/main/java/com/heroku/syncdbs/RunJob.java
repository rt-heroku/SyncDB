package com.heroku.syncdbs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.json.simple.JSONObject;

import com.heroku.syncdbs.datamover.Database;
import com.heroku.syncdbs.enums.JobStatus;
import com.heroku.syncdbs.enums.JobType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class RunJob {

	private static final String JOB_USER = "HEROKU CLI";

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		try {

			ConnectionFactory factory = new ConnectionFactory();
			factory.setUri(System.getenv("RABBITMQ_BIGWIG_URL"));

			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();
			String queueName = "" + System.getenv("QUEUE_NAME");
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("x-ha-policy", "all");
			channel.queueDeclare(queueName, true, false, false, params);

			String jobid = UUID.randomUUID().toString();
			Main main = new Main();
			main.connectBothDBs();
			
			Map<String, Integer> tables = main.getTablesToMoveFromSourceAndGetTheMaxId();
			int chunk = getChunkSize(100000);

			Database sourceDb = main.getSource();
			
			//log job
			JobLoggerHelper.logJob(sourceDb, jobid, JobType.MANUAL_CLI, JOB_USER, JobStatus.CREATED, tables.size(), chunk, main.getSourceDatabase(), main.getTargetDatabase());
			
			int indexOfTable = 0;
			
			for (String table : tables.keySet()) {
				int maxid = tables.get(table).intValue();
				List<JSONObject> tasks = new ArrayList<JSONObject>();
				int jobnum = 0;
				int jobChunk = maxid;
				int offset = 0;

				indexOfTable++;

				main.dropAndRecreateTableInTargetIfExists(table, maxid);
				
				//Analyzing jobs
				while (jobChunk > 0) {
					JSONObject obj = new JSONObject();

					jobChunk = jobChunk - chunk;
					jobnum++;

					obj.put("jobid", jobid);
					obj.put("table", table);
					obj.put("maxid", maxid);
					obj.put("offset", offset);
					obj.put("chunk", chunk);
					obj.put("jobnum", jobnum);
					
					offset = offset + chunk;

					if  (jobChunk <= 0)
						obj.replace("last", true);
					else
						obj.put("last", false);
						
					tasks.add(obj);
				}

				//Log Job details
				JobLoggerHelper.logJobDetail(sourceDb, jobid, table, indexOfTable, jobnum, maxid, JobStatus.CREATED, "");
				//Sending tasks
				for (JSONObject o : tasks){
					//Adds number of total jobs
					o.put("totaljobs", jobnum);
					//Queue task
					channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN,
							o.toJSONString().getBytes("UTF-8"));
					
					JobLoggerHelper.logInitialTask(sourceDb, o.get("table").toString(), o.get("jobid").toString(), o.get("jobnum").toString());
					
					System.out.println("MANUALLY Publishing job number[" + o.get("jobnum") + "] of " + jobnum + " jobs for TABLE[" + o.get("table")
							+ "] with total " + o.get("maxid") + " rows - OFFSET: " + o.get("offset") + " - CHUNK: " + o.get("chunk"));
					
				}

				JobLoggerHelper.logJobDetailStatus(sourceDb, jobid, table, JobStatus.SENT, indexOfTable, tasks.toString());

			}
			JobLoggerHelper.logJobStatus(sourceDb,jobid, JobStatus.SENT);

			main.closeBothConnections();
			connection.close();

		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
	}

	private static int getChunkSize(int i) {
		int r = 0;
		try {
			String cs = System.getenv("CHUNK_SIZE");
			if (cs == null || cs.equals(""))
				return i;
			r = new Integer(cs).intValue();
		} catch (Exception e) {
			return i;
		}

		return r;
	}
	

}
