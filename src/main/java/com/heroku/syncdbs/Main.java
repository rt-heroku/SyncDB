package com.heroku.syncdbs;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import com.heroku.syncdbs.datamover.DataMover;
import com.heroku.syncdbs.datamover.Database;
import com.heroku.syncdbs.datamover.DatabaseException;
import com.heroku.syncdbs.datamover.PostgreSQL;

public class Main {

	public static String SOURCE_VAR = "SOURCE_VAR";
	public static String TARGET_VAR = "TARGET_VAR";

	private DataMover mover = new DataMover();
	private Database source = new PostgreSQL();
	private Database target = new PostgreSQL();

	protected boolean isDebugEnabled() {
		String ret = System.getenv("DEBUG") + "";
		return ret.equals("TRUE");
	}

	public static void main(String[] args) throws Exception {
		Main main = new Main();
		main.copyData();
	}

	protected void copyData() throws Exception {
		try {
			long t1 = System.currentTimeMillis();
			System.out.println("Starting data mover ... " + getCurrentTime());

			connectUsingHerokuVars(getSource(), getTarget());
			
//			connectUsingJdbcUrls(source, target);
			
			getMover().setSource(getSource());
			getMover().setTarget(getTarget());

			if (isDebugEnabled()) {
				getMover().printGeneralMetadata(getSource());
				getMover().printGeneralMetadata(getTarget());
			}

			getMover().exportDatabase();

			getSource().close();
			getTarget().close();

			System.out.println("Data mover ENDED!" + getCurrentTime());
			long t2 = System.currentTimeMillis();
			System.out.println(" Took " + (t2 - t1) / 1000 + " seconds to run the job!");

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	protected void copyTableChunk(String table, Integer offset, Integer limit, Integer job) throws Exception {
		try {
			validateConnection("target");
			validateConnection("source");
			
			getMover().copyChunkTable(table, offset, limit);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	protected void copyTable(String table) throws Exception {
		try {
			long t1 = System.currentTimeMillis();
			System.out.println("Starting data mover for table [" + table + "] ... " + getCurrentTime());

			connectBothDBs();			

			if (isDebugEnabled()) {
				getMover().printGeneralMetadata(getSource());
				getMover().printGeneralMetadata(getTarget());
			}

			getMover().exportDatabase(table);

			getSource().close();
			getTarget().close();

			System.out.println("Data mover ENDED for table [" + table + "] !" + getCurrentTime());
			long t2 = System.currentTimeMillis();
			System.out.println(" Took " + (t2 - t1) / 1000 + " seconds to run the job!");

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	protected void recreateTable(String table) throws Exception {
		try {
			getMover().createTable(table);
		} catch (DatabaseException e) {
			throw new Exception(e);
		}
	}

	protected void connectUsingJdbcUrls(Database source, Database target) throws SQLException {
		source.connectString(
				"jdbc:postgresql://ec2-54-227-234-59.compute-1.amazonaws.com:5432/d3ptaja7fk91s5?user=u8ohh8b179758f&password=p2ch4dj5jkgi216ekj9cedm9lia&sslmode=require&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory");
		target.connectString(
				"jdbc:postgresql://ec2-34-192-225-110.compute-1.amazonaws.com:5432/ddtj1lkfhi8u5p?user=u2cniv4vh0r7f8&password=pbqi7smqlclupn7k8agcccmo17h&sslmode=require&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory");
	}

	protected void connectUsingHerokuVars(Database source, Database target) throws SQLException {
		connectToSource(source);
		connectToTarget(target);
	}

	public void connectBothDBs() throws Exception{
		try {
			connectUsingHerokuVars(getSource(), getTarget());
			//connectUsingJdbcUrls(getSource(), getTarget());

			getMover().setSource(getSource());
			getMover().setTarget(getTarget());
		} catch (SQLException e) {
			throw e;
		}
	}

	public void closeBothConnections() throws Exception{
		try {
			closeConnectionToSource();
			closeConnectionToTarget();
		} catch (SQLException e) {
			throw e;
		}
	}

	protected void connectToSource() throws SQLException{
		connectToSource(getSource());
	}
	
	protected void connectToTarget() throws SQLException{
		connectToTarget(getTarget());
	}
	
	protected void closeConnectionToSource() throws Exception{
		getSource().close();
	}

	protected void closeConnectionToTarget() throws Exception{
		getTarget().close();
	}
	
	protected Map<String, Integer> getTablesAndCount() throws Exception {
		try{
			validateConnection("source");
		}catch (SQLException e) {
			throw e;
		}
		return mover.getTableNameAndRowCount(getSource());
	}
	
	private void validateConnection(String db) throws SQLException{
		if (db.equals("source"))
			if (source.getConnection().isClosed())
				connectToSource(source);

		if (db.equals("target"))
			if (target.getConnection().isClosed())
				connectToSource(target);
		
	}

	protected void connectToTarget(Database target) throws SQLException {
		String target_var = System.getenv(TARGET_VAR);
		target.connect(target_var);
		System.out.println("Connected to DATABASE: " + target_var);
	}

	protected void connectToSource(Database source) throws SQLException {
		String source_var = System.getenv(SOURCE_VAR);
		source.connect(source_var);
		System.out.println("Connected to DATABASE: " + source_var);
	}

	protected static String getCurrentTime() {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		return sdf.format(cal.getTime());
	}

	public DataMover getMover() {
		return this.mover;
	}

	public void setMover(DataMover mover) {
		this.mover = mover;
	}

	public Database getSource() {
		return this.source;
	}

	public void setSource(Database source) {
		this.source = source;
	}

	public Database getTarget() {
		return this.target;
	}

	public void setTarget(Database target) {
		this.target = target;
	}


}