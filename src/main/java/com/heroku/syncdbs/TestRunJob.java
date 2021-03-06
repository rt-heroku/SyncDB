package com.heroku.syncdbs;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.heroku.syncdbs.datamover.Database;
import com.heroku.syncdbs.enums.JobStatus;
import com.heroku.syncdbs.enums.JobType;

public class TestRunJob {

	private static final String JOB_USER = "DEBUG";

	public static void main(String[] args) {
		try {
			TestRunJob rj = new TestRunJob();
			rj.runJob(JobType.MANUAL_CLI, JOB_USER);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void runJob(JobType jt, String user) throws Exception{
		SyncDB syncDB = new SyncDB();
		try {
			String jobid = UUID.randomUUID().toString();
			int chunk = getChunkSize(100000);
			int jobnum = 0;

			syncDB.connectBothDBsUsingJDBC();

			Database sourceDb = syncDB.getSource();

			if (Settings.refreshViews())
				syncDB.refreshMaterializedViews(sourceDb);
			
//			List<TableInfo> tables = syncDB.getTablesToMoveFromSourceAndGetTheMaxId();
			List<TableInfo> tables = getOnlyOneTestTableWithMaxIdEqualsToZero();

			if (Settings.analyzeBeforeProcess())
				tables = syncDB.analyzeTables(sourceDb, tables);

			JobLoggerHelper.logJob(sourceDb, jobid, jt, user, JobStatus.CREATED, tables.size(), chunk, syncDB.getSourceDatabase(), syncDB.getTargetDatabase());
			
			for (TableInfo table : tables) {
				int maxid = table.getMaxid();
				jobnum++;

				//syncDB.dropAndRecreateTableInTargetIfExists(table, maxid);

				List<JobMessage> tasks = new ArrayList<JobMessage>();

				int numOfTasks = analyzeJob(jobid, chunk, jobnum, table, maxid, tasks);

				JobLoggerHelper.logJobDetail(sourceDb, jobid, table.getName(), jobnum, numOfTasks, maxid, JobStatus.CREATED, "");

				for (JobMessage o : tasks){
					o.setTotalTasks(numOfTasks);

					JobLoggerHelper.logInitialTask(sourceDb, o);					
					logPublishingJob(numOfTasks, o);
					
				}

				JobLoggerHelper.logJobDetailStatus(sourceDb, jobid, table.getName(), JobStatus.SENT, jobnum, tasks.toString());

			}
			JobLoggerHelper.logJobStatus(sourceDb,jobid, JobStatus.SENT);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			throw e;
		} finally {
			try {
				syncDB.closeBothConnections();
			} catch (Exception e) {
				throw e;
			}
		}
	}

	private List<TableInfo> getOnlyOneTestTableWithMaxIdEqualsToZero() {
		List<TableInfo> lti = new ArrayList<TableInfo>();
		TableInfo t = new TableInfo();
		t.setSchema("transfer");
		t.setName("\"Seal_Event_Services_Governance_Approval_Request\"");
		t.setType("VIEW");
		t.setCount(0);
		t.setRefresh(false);
		t.setAnalyze(false);
		t.setMaxid(0);
		lti.add(t);

		return lti;
	}
	private int analyzeJob(String jobid, int chunk, int jobnum, TableInfo table, int maxid, List<JobMessage> tasks) {
		int tasknum = 0;
		int jobChunk = maxid;
		int offset = 0;
		
		while (jobChunk >= 0) {
			JobMessage jm = new JobMessage();

			jobChunk = jobChunk - chunk;
			tasknum++;

			jm.setJobid(jobid);
			jm.setTable(table);
			jm.setMaxid(maxid);
			jm.setOffset(offset);
			jm.setChunk(chunk);
			jm.setTasknum(tasknum);
			jm.setJobnum(jobnum);
			
			offset = offset + chunk;

			if  (jobChunk <= 0)
				jm.setLast(true);
			else
				jm.setLast(false);
				
			tasks.add(jm);
		}
		return tasknum;
	}

	private static void logPublishingJob(int jobnum, JobMessage o) {
		System.out.println("MANUALLY Publishing task number[" + o.getTasknum() + "] of " + jobnum + " tasks for TABLE[" + o.getTable()
				+ "] with total " + o.getMaxid() + " rows - OFFSET: " + o.getOffset() + " - CHUNK: " + o.getChunk());
	}

	private static int getChunkSize(int i) {
		int r = 0;
		try {
			String cs = Settings.getChunkSize();
			if (cs == null || cs.equals(""))
				return i;
			r = new Integer(cs).intValue();
		} catch (Exception e) {
			return i;
		}

		return r;
	}
}