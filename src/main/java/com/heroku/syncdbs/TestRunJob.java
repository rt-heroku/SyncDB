package com.heroku.syncdbs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.heroku.syncdbs.datamover.Database;
import com.heroku.syncdbs.enums.JobStatus;
import com.heroku.syncdbs.enums.JobType;

public class TestRunJob {

	private static final String JOB_USER = "HEROKU CLI";

	public static void main(String[] args) {
		SyncDB syncDB = new SyncDB();

		try {

			String jobid = UUID.randomUUID().toString();
			syncDB.connectBothDBsUsingJDBC();

			Map<String, Integer> tables = syncDB.getTablesToMoveFromSourceAndGetTheMaxId();
			int chunk = getChunkSize(100000);

			Database sourceDb = syncDB.getSource();

			int indexOfTable = 0;
			JobLoggerHelper.logJob(sourceDb, jobid, JobType.MANUAL_CLI, JOB_USER, JobStatus.CREATED, tables.size(), chunk, syncDB.getSourceDatabase(), syncDB.getTargetDatabase());

			for (String table : tables.keySet()) {
				List<JobMessage> tasks = new ArrayList<JobMessage>();
				int maxid = tables.get(table).intValue();
				int jobnum = 0;
				int jobChunk = maxid;
				int offset = 0;

				indexOfTable++;

				syncDB.dropAndRecreateTableInTargetIfExists(table, maxid);
				
				//Analyzes jobs
				while (jobChunk > 0) {
					JobMessage jm = new JobMessage();

					jobChunk = jobChunk - chunk;
					jobnum++;

					jm.setJobid(jobid);
					jm.setTable(table);
					jm.setMaxid(maxid);
					jm.setOffset(offset);
					jm.setChunk(chunk);
					jm.setJobnum(jobnum);

					logPublishingJob(jobnum, jm);
					
					offset = offset + chunk;

					if  (jobChunk <= 0)
						jm.setLast(true);
					else
						jm.setLast(false);
						
					tasks.add(jm);
				}

				// Log Job details
				JobLoggerHelper.logJobDetail(sourceDb, jobid, table, indexOfTable, jobnum, maxid, JobStatus.CREATED, "");
				JobLoggerHelper.logJobStatus(sourceDb, jobid, JobStatus.ANALYZED);
				// Sending tasks
				for (JobMessage o : tasks){
					//Adds number of total jobs before sending
					o.setTotalJobs(jobnum);

					JobLoggerHelper.logInitialTask(sourceDb, o);

					System.out.println(o.toJson());
				}
				JobLoggerHelper.logJobDetailStatus(sourceDb, jobid, table, JobStatus.SENT, indexOfTable, tasks.toString());
			}
			JobLoggerHelper.logJobStatus(sourceDb, jobid, JobStatus.SENT);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				syncDB.closeBothConnections();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	private static void logPublishingJob(int jobnum, JobMessage o) {
		System.out.println("MANUALLY Publishing job number[" + o.getJobnum() + "] of " + jobnum + " jobs for TABLE[" + o.getTable()
				+ "] with total " + o.getMaxid() + " rows - OFFSET: " + o.getOffset() + " - CHUNK: " + o.getChunk());
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
