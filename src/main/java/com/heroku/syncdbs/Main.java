package com.heroku.syncdbs;

import com.heroku.syncdbs.datamover.DataMover;
import com.heroku.syncdbs.datamover.Database;
import com.heroku.syncdbs.datamover.DatabaseException;
import com.heroku.syncdbs.datamover.PostgreSQL;

public class Main {
    
    public static void main(String[] args) throws Exception {
        
//        Connection sourceConn = getSourceConnection();
//        Connection targetConn = getTargetConnection();
//        
//        Statement stmtSource = sourceConn.createStatement();
//        Statement stmtTarget = targetConn.createStatement();
//
//        testDatabase(stmtSource);
//        testDatabase(stmtTarget);
//        
//        stmtSource.close();
//        stmtTarget.close();
//        
//        sourceConn.close();
//        targetConn.close();
//        
//        System.out.println("DONE!");
//    }
//
//    private static void copyTable(String table) throws Exception{
		try {
			DataMover mover = new DataMover();

			Database source = new PostgreSQL();
			source.connect("JDBC_DATABASE_URL");

			Database target = new PostgreSQL();
			target.connect("HEROKU_POSTGRESQL_AMBER_JDBC_URL");

			mover.setSource(source);
			mover.setTarget(target);
			
			mover.copyTableData("servicesrule");
			
			source.close();
			target.close();

		} catch (DatabaseException e) {
			e.printStackTrace();
			throw e;
		}

    }
/*    
	private static void testDatabase(Statement stmt) throws SQLException {
		stmt.executeUpdate("DROP TABLE IF EXISTS ticks");
        stmt.executeUpdate("CREATE TABLE ticks (tick timestamp)");
        stmt.executeUpdate("INSERT INTO ticks VALUES (now())");
        ResultSet rs = stmt.executeQuery("SELECT tick FROM ticks");
        while (rs.next()) {
            System.out.println("Read from DB[" + stmt.getConnection().getMetaData().getURL() + "]: " + rs.getTimestamp("tick"));
        }
        
        rs.close();
	}
    
    private static Connection getSourceConnection() throws URISyntaxException, SQLException {
    	String url = System.getenv("JDBC_DATABASE_URL") + "";
        URI dbUri = new URI(System.getenv("DATABASE_URL"));

        String username = dbUri.getUserInfo().split(":")[0];
        String password = dbUri.getUserInfo().split(":")[1];
        String dbUrl = "jdbc:postgresql://" + dbUri.getHost() + dbUri.getPath();

        if (url.equals(""))
        	return DriverManager.getConnection(dbUrl, username, password);
        else
        	return DriverManager.getConnection(url);
        
    }

    private static Connection getTargetConnection() throws URISyntaxException, SQLException {
    	String url = System.getenv("HEROKU_POSTGRESQL_AMBER_JDBC_URL") + "";
        URI dbUri = new URI(System.getenv("HEROKU_POSTGRESQL_AMBER_URL"));

        String username = dbUri.getUserInfo().split(":")[0];
        String password = dbUri.getUserInfo().split(":")[1];
        String dbUrl = "jdbc:postgresql://" + dbUri.getHost() + dbUri.getPath();

        if (url.equals(""))
        	return DriverManager.getConnection(dbUrl, username, password);
        else
        	return DriverManager.getConnection(url);
        
    }
    */
}