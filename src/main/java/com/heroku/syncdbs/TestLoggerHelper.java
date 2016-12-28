package com.heroku.syncdbs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.json.simple.JSONObject;

import com.heroku.syncdbs.datamover.Database;
import com.heroku.syncdbs.enums.JobStatus;
import com.heroku.syncdbs.enums.JobType;

public class TestLoggerHelper {
	private static final String JOB_USER = "HEROKU CLI TESTER";

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		String jobid = UUID.randomUUID().toString();
		Main main = new Main();
		main.connectBothDBsUsingJDBC();
		
		Database sourceDb = main.getSource();
		
		int chunk = 20000;
		int indexOfTable = 0;
		
		Map<String, Integer> data = getTableTestData();
		Set<String> tables = data.keySet();
		//TEST 1
		JobLoggerHelper.logJob(sourceDb, jobid, JobType.MANUAL_CLI, JOB_USER, JobStatus.CREATED, tables.size(), chunk, main.getSourceDatabase(), main.getTargetDatabase());
		JobLoggerHelper.logJobStatus(sourceDb, jobid, JobStatus.CREATED);

		//TEST 2
		for (String table : data.keySet()) {
			int count = data.get(table).intValue();
			List<JSONObject> tasks = new ArrayList<JSONObject>();
			
			int job = 0;
			int jobChunk = count;
			int offset = 0;

			indexOfTable++;
			//Analyzing jobs
			while (jobChunk > 0) {
				JSONObject obj = new JSONObject();

				jobChunk = jobChunk - chunk;
				job++;

				obj.put("jobid", jobid);
				obj.put("table", table);
				obj.put("maxid", count);
				obj.put("offset", offset);
				obj.put("chunk", chunk);
				obj.put("jobnum", job);

				offset = offset + chunk;

				if  (!(jobChunk > 0))
					obj.put("last", true);
				else
					obj.put("last", false);
					
				tasks.add(obj);
			}
			
			//Log Job details
			JobLoggerHelper.logJobDetail(sourceDb, jobid, table, indexOfTable, job, count, JobStatus.CREATED, "");

			JobLoggerHelper.logJobStatus(sourceDb, jobid, JobStatus.ANALYZED);
			//Sending tasks
			for (JSONObject o : tasks){
				o.put("totaljobs", job);

				JobLoggerHelper.logInitialTask(sourceDb, o.get("table").toString(), o.get("jobid").toString(), o.get("jobnum").toString());
				
			}
			JobLoggerHelper.logJobStatus(sourceDb,jobid, JobStatus.SENT);

			//Emulates Task processing in Worker!!!			
			for (JSONObject o : tasks){
				JobLoggerHelper.logTask(sourceDb, jobid, getIntValue(o.get("jobnum")),table,1000);
				System.out.println(o.toJSONString());
			}
			JobLoggerHelper.logJobDetailStatus(sourceDb, jobid, table, JobStatus.SENT, indexOfTable, tasks.toString());
		}
		JobLoggerHelper.logJobStatus(sourceDb,jobid, JobStatus.FINISHED);
		
	}
	
	private static int getIntValue(Object o) {
		try {
			Integer i = new Integer(o.toString());
			return i.intValue();
		} catch (Exception e) {
			return 0;
		}
	}

	private static Map<String, Integer> getTableTestData(){
		Map<String, Integer> ts = new HashMap<String, Integer>();
		
		for (int i=1; i<=10; i++){
			ts.put("table_" + i, getRandomCount());
		}
		return ts;
	}
	
	private static int getRandomCount(){
		Random rand = new Random();

		int  n = rand.nextInt(200000) + 50000;
		return n;
	}

}
