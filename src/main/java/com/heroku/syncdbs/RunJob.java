package com.heroku.syncdbs;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Date;
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
			logJob(main.getTarget(), jobid, JobType.MANUAL_CLI, JOB_USER, JobStatus.CREATED, tables.size(), chunk, main.getSourceDatabase(), main.getTargetDatabase());
			
			for (String table : tables.keySet()) {
				int count = tables.get(table).intValue();
				JSONObject obj = new JSONObject();
				List<JSONObject> tasks = new ArrayList<JSONObject>();
				
				int job = 0;
				int jobChunk = count;
				int offset = 0;

				main.dropAndRecreateTableInTargetIfExists(table, count);
				
				//Analyzing jobs
				while (jobChunk > 0) {

					jobChunk = jobChunk - chunk;
					job++;

					obj.put("jobid", jobid);
					obj.put("table", table);
					obj.put("maxid", count);
					obj.put("offset", offset);
					obj.put("chunk", chunk);
					obj.put("jobnum", job);

					tasks.add(obj);
					
					offset = offset + chunk;
				}
				
				//Log Job details
				logJobDetail(sourceDb, jobid, table, job, count, JobStatus.CREATED);
				logJobStatus(sourceDb, jobid, JobStatus.ANALYZED);
				//Sending tasks
				for (JSONObject o : tasks){
					//Adds number of total jobs
					o.put("totaljobs", job);
					//Queue task
//					channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN,
//							o.toJSONString().getBytes("UTF-8"));
					
					logTask(sourceDb, o.get("table").toString(), o.get("jobid").toString(), o.get("jobnum").toString());
					
					System.out.println("MANUALLY Publishing job number[" + o.get("jobnum") + "] of " + job + " jobs for TABLE[" + o.get("table")
							+ "] with total " + o.get("maxid") + " rows - OFFSET: " + o.get("offset") + " - CHUNK: " + o.get("chunk"));
					
				}
				logJobStatus(sourceDb,jobid, JobStatus.SENT);

			}

			main.closeBothConnections();
			connection.close();

		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
	}

	private static java.sql.Date getNow(){
		Date d = new Date();
		return new java.sql.Date(d.getTime());
	}
	
	private static void logJobStatus(Database db, String jobid, JobStatus jobStatus) {
				try{

					String sql = "UPDATE syncdb.job SET status = ?,  WHERE jobid = ?";
					PreparedStatement st = db.prepareStatement(sql);;

					st.setDate(1, getNow());
					st.setString(2, jobStatus.name());		
					st.setString(10, jobid);
					
					st.execute();
					
				}catch (Exception e) {
					System.err.println("Error logging Job - " + e.getMessage());
					e.printStackTrace();
				}
		
	}

	private static void logJobDetail(Database db, String jobid, String table, int numOfJobs, int maxid, JobStatus created) {
		
	}

	private static void logTask(Database db, String table, String jobId, String taskNum){
		try {
			String sql = "INSERT INTO syncdb.task (jobid, table, tasknum, index_loaded) VALUES('" + jobId + "','" + table + "'," + taskNum + ",0)";
			db.execute(sql);
		} catch (Exception e) {
			System.err.println("Error while logging table [" + table + "] load for job id [" + jobId + "]" + e.getMessage());
		}
		
	}


	private static void logJob(Database database, String jobid, JobType jobType, String jobUser, JobStatus jobStatus, int numOfTasks, int chunkSize, String sourceDB, String targetDB) {
/**

logJob(main.getTarget(), jobid, JobType.MANUAL_CLI, JOB_USER, JobStatus.CREATED, tables.size(), chunk, main.getSourceDatabase(), main.getTargetDatabase());

CREATE TABLE job
(
   id             integer,
   job_start      timestamp (5) WITHOUT TIME ZONE,
   job_end        timestamp (6) WITHOUT TIME ZONE,
   status         CHARACTER VARYING (20),
   num_of_tasks   integer,
   chunk_size     integer,
   db_from        CHARACTER VARYING (20),
   db_to          CHARACTER VARYING (20),
*   next_job       timestamp (6) WITHOUT TIME ZONE,
   type           CHARACTER VARYING (20),
   "user"         CHARACTER VARYING (20),
   jobid          CHARACTER VARYING (50)
);
 */
		try{

			String sql = "INSERT INTO syncdb.job (job_start,status,num_of_tasks,chunk_size,db_from, db_to, next_job,type ,\"user\",jobid) VALUES (?,?,?,?,?,?,?,?,?,?);";
			PreparedStatement st = database.prepareStatement(sql);;

			st.setDate(1, getNow());
			st.setString(2, jobStatus.name());		
			st.setInt(3, numOfTasks);
			st.setInt(4, chunkSize);
			st.setString(5, sourceDB);
			st.setString(6, targetDB);
			st.setDate(7, getNow());
			st.setString(8, jobType.name());
			st.setString(9, jobUser);
			st.setString(10, jobid);
			
			st.execute();
			
		}catch (Exception e) {
			System.err.println("Error logging Job - " + e.getMessage());
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
