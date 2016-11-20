package com.heroku.syncdbs;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import com.heroku.syncdbs.datamover.DataMover;
import com.heroku.syncdbs.datamover.Database;
import com.heroku.syncdbs.datamover.PostgreSQL;

public class Main {

	public static String SOURCE_VAR = "SOURCE_VAR";
	public static String TARGET_VAR = "TARGET_VAR";

	private static DataMover mover = new DataMover();
	private static Database source = new PostgreSQL();
	private static Database target = new PostgreSQL();

	protected static boolean isDebugEnabled() {
		String ret = System.getenv("DEBUG") + "";
		return ret.equals("TRUE");
	}

	public static void main(String[] args) throws Exception {
		copyData();
	}

	protected static void copyData() throws Exception {
		try {
			long t1 = System.currentTimeMillis();
			System.out.println("Starting data mover ... " + getCurrentTime());

			connectUsingHerokuVars(getSource(), getTarget());

			getMover().setSource(getSource());
			getMover().setTarget(getTarget());

			if (isDebugEnabled()) {
				getMover().printGeneralMetadata(getSource());
				getMover().printGeneralMetadata(getTarget());
			}

			getSource().getConnection().setAutoCommit(false);
			getTarget().getConnection().setAutoCommit(false);

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

	protected static void connectUsingJdbcUrls(Database source, Database target) throws SQLException {
		source.connectString(
				"jdbc:postgresql://ec2-52-73-169-99.compute-1.amazonaws.com:5432/d3ptaja7fk91s5?user=u8ohh8b179758f&password=p2ch4dj5jkgi216ekj9cedm9lia&sslmode=require&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory");
		target.connectString(
				"jdbc:postgresql://ec2-52-200-41-184.compute-1.amazonaws.com:5432/d9mgkh21nofekg?user=uegso4e2g4jqof&password=p991t3gs4ehj3ublia03ssn3jgs&sslmode=require&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory");
	}

	protected static void connectUsingHerokuVars(Database source, Database target) throws SQLException {
		connectToSource(source);
		connectToTarget(target);
	}

	protected static void connectToSource() throws SQLException{
		connectToSource(getSource());
	}
	
	protected static void connectToTarget() throws SQLException{
		connectToTarget(getTarget());
	}
	
	protected static Map<String, Integer> getTablesAndCount() throws Exception {
		connectToSource();
		Map<String, Integer> m = mover.getTableNameAndRowCount(getSource());
		getSource().close();
		return m;
	}
	
	protected static void connectToTarget(Database target) throws SQLException {
		String target_var = System.getenv(TARGET_VAR);
		target.connect(target_var);
		System.out.println("Connected to DATABASE: " + target_var);
	}

	protected static void connectToSource(Database source) throws SQLException {
		String source_var = System.getenv(SOURCE_VAR);
		source.connect(source_var);
		System.out.println("Connected to DATABASE: " + source_var);
	}

	protected static String getCurrentTime() {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		return sdf.format(cal.getTime());
	}

	public static DataMover getMover() {
		return mover;
	}

	public static void setMover(DataMover mover) {
		Main.mover = mover;
	}

	public static Database getSource() {
		return source;
	}

	public static void setSource(Database source) {
		Main.source = source;
	}

	public static Database getTarget() {
		return target;
	}

	public static void setTarget(Database target) {
		Main.target = target;
	}

}