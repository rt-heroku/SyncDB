package com.heroku.syncdbs;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.heroku.syncdbs.datamover.Database;
import com.heroku.syncdbs.datamover.DatabaseException;
import com.heroku.syncdbs.enums.JobStatus;
import com.heroku.syncdbs.enums.JobType;

public class JobLoggerHelper {

	private static java.sql.Date getNow() {
		Date d = new Date();
		return new java.sql.Date(d.getTime());
	}

	private static Timestamp getTimestampNow() {
		Date d = new Date();
		return new Timestamp(d.getTime());
	}

	public static void logJobStatus(Database db, String jobid, JobStatus jobStatus) {
		try {

			String sql = "UPDATE syncdb.job SET status = ?, status_date = ? WHERE jobid = ?";
			PreparedStatement st = db.prepareStatement(sql);

			st.setString(1, jobStatus.name());
			st.setTimestamp(2, getTimestampNow());
			st.setString(3, jobid);

			st.execute();
			st.close();
			System.out.println("Job [" + jobid + "] - " + jobStatus.name());

		} catch (Exception e) {
			System.err.println("Error logging Job - " + e.getMessage());
			e.printStackTrace();
		}

	}
	public static void logJobStatusAndTables(Database db, String jobid, JobStatus jobStatus, int tables) {
		try {
			
			String sql = "UPDATE syncdb.job SET status = ?, status_date = ?, num_of_jobs = ? WHERE jobid = ?";
			PreparedStatement st = db.prepareStatement(sql);
			
			st.setString(1, jobStatus.name());
			st.setTimestamp(2, getTimestampNow());
			st.setInt(3, tables);
			st.setString(4, jobid);
			
			st.execute();
			st.close();
			System.out.println("Job [" + jobid + "] - " + jobStatus.name());
			
		} catch (Exception e) {
			System.err.println("Error logging Job - " + e.getMessage());
			e.printStackTrace();
		}
		
	}

	public static void logEndOfJob(Database db, String jobid) {
		try {

			String sql = "UPDATE syncdb.job SET status = ?, status_date = ?, job_end = ? WHERE jobid = ?";
			PreparedStatement st = db.prepareStatement(sql);

			st.setString(1, JobStatus.FINISHED.name());
			st.setTimestamp(2, getTimestampNow());
			st.setTimestamp(3, getTimestampNow());
			st.setString(4, jobid);

			st.executeUpdate();
			st.close();
			System.out.println("Job [" + jobid + "] - FINISHED");

		} catch (Exception e) {
			System.err.println("Error logging Job - " + e.getMessage());
			e.printStackTrace();
		}

	}
	public static void logJobDetailStatus(Database db, String jobid, String table, JobStatus jobStatus,
			int jobNum, String comment) {
		try {

			String sql = "UPDATE syncdb.job_detail SET status = ?, status_date = ?, comment = ? WHERE jobid = ? and \"table\" =  ? and job_num = ?";
			PreparedStatement st = db.prepareStatement(sql);

			st.setString(1, jobStatus.name());
			st.setTimestamp(2, getTimestampNow());
			st.setString(3, comment);
			st.setString(4, jobid);
			st.setString(5, table);
			st.setInt(6, jobNum);

			st.executeUpdate();
			st.close();
			System.out.println("JobDetail [" + jobid + "] - " + jobStatus.name());

		} catch (Exception e) {
			System.err.println("Error logging Job - " + e.getMessage());
			e.printStackTrace();
		}

	}

	public static void logJobDetail(Database db, String jobid, String table, int jobNum, int numOfJobs, int maxid,
			JobStatus status, String comment) {
		try {

			String sql = "INSERT INTO syncdb.job_detail (jobid, status_date, status, comment, \"table\", num_of_tasks, job_num, maxid) VALUES (?,?,?,?,?,?,?,?);";
			PreparedStatement st = db.prepareStatement(sql);

			Timestamp t = getTimestampNow();

			st.setString(1, jobid);
			st.setTimestamp(2, t);
			st.setString(3, status.name());
			st.setString(4, comment);
			st.setString(5, table);
			st.setInt(6, numOfJobs);
			st.setInt(7, jobNum);
			st.setInt(8, maxid);

			st.executeUpdate();
			st.close();

		} catch (Exception e) {
			System.err.println("Error logging Job - " + e.getMessage());
			e.printStackTrace();
		}

	}

	public static void logInitialTask(Database db, JobMessage jm) {
		logInitialTask(db, jm.getTable().getFullName(), jm.getJobid(), jm.getTasknum());
	}
	public static void logInitialTask(Database db, String table, String jobId, int taskNum) {
		try {
			String sql = "INSERT INTO syncdb.task (jobid, \"table\", tasknum, index_loaded, status, status_date) VALUES(?,?,?,?,?,?)";
			PreparedStatement st = db.prepareStatement(sql);
			st.setString(1, jobId);
			st.setString(2, table);
			st.setInt(3, taskNum);
			st.setInt(4, 0);
			st.setString(5, JobStatus.SENT.name());
			st.setTimestamp(6, getTimestampNow());
			st.executeUpdate();
			st.close();
		} catch (Exception e) {
			System.err.println(
					"Error while logging table [" + table + "] load for job id [" + jobId + "]" + e.getMessage());
		}

	}

	public static void logJob(Database database, String jobid, JobType jobType, String jobUser, JobStatus jobStatus,
			int numOfTasks, int chunkSize, String sourceDB, String targetDB) {
		try {

			String sql = "INSERT INTO syncdb.job (job_start,status,num_of_jobs,chunk_size,db_from, db_to, next_job,type ,\"user\",jobid, status_date) VALUES (?,?,?,?,?,?,?,?,?,?,?);";
			PreparedStatement st = database.prepareStatement(sql);

			Timestamp t = getTimestampNow();

			st.setTimestamp(1, t);
			st.setString(2, jobStatus.name());
			st.setInt(3, numOfTasks);
			st.setInt(4, chunkSize);
			st.setString(5, sourceDB);
			st.setString(6, targetDB);
			st.setDate(7, getNow());
			st.setString(8, jobType.name());
			st.setString(9, jobUser);
			st.setString(10, jobid);
			st.setTimestamp(11, t);

			st.executeUpdate();
			st.close();

		} catch (Exception e) {
			System.err.println("Error logging Job - " + e.getMessage());
			e.printStackTrace();
		}

	}

	public static boolean tasknumExistsInTable(Database db, String jobid, String table, String column, int value)
			throws Exception {
		ResultSet rs = null;
		boolean ret = false;
		String sql = "SELECT " + column + " FROM syncdb.task WHERE jobid='" + jobid + "' AND \"table\"='" + table
				+ "' and " + column + "=" + value;
		rs = db.prepareStatement(sql).executeQuery();
		ret = rs.next();
		rs.close();
		return ret;
	}

	public static void logTask(Database db, String jobId, int taskNum, String table, int rows) {
		try {
			String sql = "";
			if (tasknumExistsInTable(db, jobId, table, "tasknum", taskNum)) {
				sql = "UPDATE syncdb.task SET index_loaded = " + rows + " WHERE jobid='" + jobId + "' AND \"table\"='"
						+ table + "' AND tasknum=" + taskNum;
			} else {
				sql = "INSERT INTO syncdb.task (jobid, \"table\", tasknum, index_loaded) VALUES('" + jobId + "','"
						+ table + "'," + taskNum + "," + rows + ")";
			}
			db.execute(sql);
		} catch (Exception e) {
			System.err.println(
					"Error while logging table [" + table + "] load for job id [" + jobId + "]" + e.getMessage());
		}
	}
	public static void logTaskStatus(Database db, String jobId, int taskNum, String table, JobStatus j) {
		try {
			String sql = "UPDATE syncdb.task SET status = ?, status_date = ? WHERE jobid = ? and \"table\" =  ? and tasknum = ?";
			PreparedStatement st = db.prepareStatement(sql);

			st.setString(1, j.name());
			st.setTimestamp(2, getTimestampNow());
			st.setString(3, jobId);
			st.setString(4, table);
			st.setInt(5, taskNum);

			st.executeUpdate();
			System.out.println("Rows updated: " + st.getUpdateCount() + "\n" + st);
			st.close();

		} catch (Exception e) {
			System.err.println("Error logging Job - " + e.getMessage());
			e.printStackTrace();
		}

	}
	public static void analyzeJobTask(Database db, JobMessage jm) throws SQLException, DatabaseException {
		int jobs = jm.getTotalTasks();
		int count = countFinishedTasks(db, jm);
		if (count == jobs){
			logJobDetailStatus(db, jm.getJobid(), jm.getTable().getFullName(), JobStatus.FINISHED, jm.getJobnum(), "");
			analyzeJobDetails(db, jm);
		}
	}

	private static void analyzeJobDetails(Database db, JobMessage jm) throws SQLException, DatabaseException {
		int jobs = getNumOfJobs(db, jm.getJobid());
		int count = countFinishedJobs(db, jm);
		
		if(count == jobs)
			logEndOfJob(db, jm.getJobid());
		
	}

	private static int getNumOfJobs(Database db, String jobid) throws SQLException, DatabaseException {
		ResultSet rs = null;
		int count = 0;
		String sql = "SELECT num_of_jobs FROM syncdb.job WHERE jobid='" + jobid + "'";
		rs = db.prepareStatement(sql).executeQuery();
		if (rs.next())
			count = rs.getInt(1);
		rs.close();
		return count;
	}

	private static int countFinishedTasks(Database db, JobMessage jm) throws SQLException, DatabaseException{
		ResultSet rs = null;
		int count = 0;
		String sql = "SELECT count(*) FROM syncdb.task WHERE jobid='" + jm.getJobid() + "' AND \"table\"='" + jm.getTable().getFullName()
				+ "' and status = '" + JobStatus.FINISHED.name() + "'";
		rs = db.prepareStatement(sql).executeQuery();
		if (rs.next())
			count = rs.getInt(1);
		rs.close();
		return count;
	}
	
	public static int countFinishedJobs(Database db, JobMessage jm) throws SQLException, DatabaseException{
		ResultSet rs = null;
		int count = 0;
		String sql = "SELECT count(*) FROM syncdb.job_detail WHERE jobid='" + jm.getJobid() + "' and status = '" + JobStatus.FINISHED.name() + "'";
		rs = db.prepareForwardStatement(sql).executeQuery();
		if (rs.next())
			count = rs.getInt(1);
		rs.close();
		return count;
	}

	public static List<Map<String,String>> getFinishedJobs(Database db, JobMessage jm) throws SQLException, DatabaseException{
		ResultSet rs = null;
		List<Map<String,String>> ret = new ArrayList<Map<String,String>>();
		String sql = "SELECT \"table\", status FROM syncdb.job_detail WHERE jobid='" + jm.getJobid() + "'";
		rs = db.prepareForwardStatement(sql).executeQuery();
		while (rs.next()){
			Map<String,String> m = new HashMap<String, String>();
			m.put(rs.getString(1), rs.getString(2));
			System.out.println(rs.getString(1) + " - " + rs.getString(2));
			ret.add(m);
		}
		rs.close();
		return ret;
	}
	
}
