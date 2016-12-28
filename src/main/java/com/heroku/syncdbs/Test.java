package com.heroku.syncdbs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.json.simple.JSONObject;

import com.heroku.syncdbs.datamover.Database;
import com.heroku.syncdbs.enums.JobStatus;
import com.heroku.syncdbs.enums.JobType;

public class Test {

	private static final String JOB_USER = "HEROKU CLI";

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		try {

			String jobid = UUID.randomUUID().toString();
			Main main = new Main();
			main.connectBothDBsUsingJDBC();

			Map<String, Integer> tables = main.getTablesToMoveFromSourceAndGetTheMaxId();
			int chunk = getChunkSize(100000);

			Database sourceDb = main.getSource();

			int indexOfTable = 0;
			// log job
			JobLoggerHelper.logJob(sourceDb, jobid, JobType.MANUAL_CLI, JOB_USER, JobStatus.CREATED, tables.size(), chunk,
					main.getSourceDatabase(), main.getTargetDatabase());

			for (String table : tables.keySet()) {
				int count = tables.get(table).intValue();
				JSONObject obj = new JSONObject();
				List<JSONObject> tasks = new ArrayList<JSONObject>();

				int job = 0;
				int jobChunk = count;
				int offset = 0;

				indexOfTable++;
				
				main.dropAndRecreateTableInTargetIfExists(table, count);

				while (jobChunk > 0) {

					jobChunk = jobChunk - chunk;
					job++;

					obj.put("table", table);
					obj.put("count", count);
					obj.put("offset", offset);
					obj.put("limit", chunk);
					obj.put("job", job);

					System.out.println("MANUALLY Publishing job number[" + job + "] for TABLE[" + table
							+ "] with total " + count + " rows - OFFSET: " + offset + " - LIMIT: " + chunk);

					offset = offset + chunk;
					
					if  (jobChunk <= 0)
						obj.replace("last", true);
					else
						obj.put("last", false);
						
					tasks.add(obj);
				}

				// Log Job details
				JobLoggerHelper.logJobDetail(sourceDb, jobid, table, indexOfTable, job, count, JobStatus.CREATED, "");
				JobLoggerHelper.logJobStatus(sourceDb, jobid, JobStatus.ANALYZED);
				// Sending tasks
				for (JSONObject o : tasks) {
					// Adds number of total jobs
					o.put("totaljobs", job);
					// Queue task
					// channel.basicPublish("", queueName,
					// MessageProperties.PERSISTENT_TEXT_PLAIN,
					// o.toJSONString().getBytes("UTF-8"));

					JobLoggerHelper.logInitialTask(sourceDb, o.get("table").toString(), o.get("jobid").toString(), o.get("jobnum").toString());

//					System.out.println("MANUALLY Publishing job number[" + o.get("jobnum") + "] of " + job
//							+ " jobs for TABLE[" + o.get("table") + "] with total " + o.get("maxid")
//							+ " rows - OFFSET: " + o.get("offset") + " - CHUNK: " + o.get("chunk"));
					System.out.println(o.toJSONString());
				}
				JobLoggerHelper.logJobDetailStatus(sourceDb, jobid, table, JobStatus.SENT, indexOfTable, tasks.toString());
			}
			JobLoggerHelper.logJobStatus(sourceDb, jobid, JobStatus.SENT);
			main.closeBothConnections();
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
