package com.heroku.syncdbs;

import java.sql.SQLException;

import com.heroku.syncdbs.datamover.DataMover;
import com.heroku.syncdbs.datamover.Database;
import com.heroku.syncdbs.datamover.DatabaseException;
import com.heroku.syncdbs.datamover.PostgreSQL;

public class Main {
    public static String SOURCE_VAR="HEROKU_POSTGRESQL_GRAY_JDBC_URL";
    public static String TARGET_VAR="HEROKU_POSTGRESQL_JADE_JDBC_URL";
    
    public static void main(String[] args) throws Exception {
		try {
			
			System.out.println("Starting data mover ... ");
			DataMover mover = new DataMover();

			//gray database
			Database source = new PostgreSQL();
			//JADE database
			Database target = new PostgreSQL();

			connectUsingHerokuVars(source, target);

//			connectUsingJdbcUrls(source, target);
			
			mover.setSource(source);
			mover.setTarget(target);
			
			mover.createTablesFromViews();
			
//			mover.exportDatabse();
			//mover.copyTableData("servicesrule_1");
			mover.copyViewToTableData("servicesrule_1");
			
			source.close();
			target.close();

		} catch (DatabaseException e) {
			e.printStackTrace();
			throw e;
		}

    }
	private static void connectUsingJdbcUrls(Database source, Database target) throws SQLException {
		source.connectString("jdbc:postgresql://ec2-52-73-169-99.compute-1.amazonaws.com:5432/d3ptaja7fk91s5?user=u8ohh8b179758f&password=p2ch4dj5jkgi216ekj9cedm9lia&sslmode=require&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory");
		target.connectString("jdbc:postgresql://ec2-54-243-47-213.compute-1.amazonaws.com:5432/dfh0t3febn05fs?user=vmblscrfgwnpal&password=gIFhkN66JVvthBl47Utvxxxm9J&sslmode=require&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory");
	}

	private static void connectUsingHerokuVars(Database source, Database target) throws SQLException {
		source.connect(SOURCE_VAR);
		System.out.println("Connected to DATABSE: " + SOURCE_VAR);

		target.connect(TARGET_VAR);
		System.out.println("Connected to DATABSE: " + TARGET_VAR);
	}
}