package com.heroku.syncdbs;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;

public class Main {
    
    public static void main(String[] args) throws Exception {
        
        Connection sourceConn = getSourceConnection();
        Connection targetConn = getTargetConnection();
        
        Statement stmtSource = sourceConn.createStatement();
        Statement stmtTarget = sourceConn.createStatement();

        testDatabase(stmtSource);
        testDatabase(stmtTarget);
        
        stmtSource.close();
        stmtTarget.close();
        
        sourceConn.close();
        targetConn.close();
        
        System.out.println("DONE!");
    }

	private static void testDatabase(Statement stmt) throws SQLException {
		stmt.executeUpdate("DROP TABLE IF EXISTS ticks");
        stmt.executeUpdate("CREATE TABLE ticks (tick timestamp)");
        stmt.executeUpdate("INSERT INTO ticks VALUES (now())");
        ResultSet rs = stmt.executeQuery("SELECT tick FROM ticks");
        while (rs.next()) {
            System.out.println("Read from DB[" + stmt.getConnection().getCatalog() + "]: " + rs.getTimestamp("tick"));
        }
        
        rs.close();
	}
    
    private static Connection getSourceConnection() throws URISyntaxException, SQLException {
        URI dbUri = new URI(System.getenv("JDBC_DATABASE_URL"));

        String username = dbUri.getUserInfo().split(":")[0];
        String password = dbUri.getUserInfo().split(":")[1];
        String dbUrl = "jdbc:postgresql://" + dbUri.getHost() + dbUri.getPath();

        return DriverManager.getConnection(dbUrl, username, password);
    }

    private static Connection getTargetConnection() throws URISyntaxException, SQLException {
        URI dbUri = new URI(System.getenv("JDBC_DATABASE_URL"));

        String username = dbUri.getUserInfo().split(":")[0];
        String password = dbUri.getUserInfo().split(":")[1];
        String dbUrl = "jdbc:postgresql://" + dbUri.getHost() + dbUri.getPath();

        return DriverManager.getConnection(dbUrl, username, password);
    }
}