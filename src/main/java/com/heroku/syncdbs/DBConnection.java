package com.heroku.syncdbs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {

	private static String DB_URL = "jdbc:postgresql://ec2-52-73-169-99.compute-1.amazonaws.com:5432/d3ptaja7fk91s5?user=u8ohh8b179758f&password=p2ch4dj5jkgi216ekj9cedm9lia&sslmode=require&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";

	public static Connection getConnection() throws SQLException {
		Connection connection = DriverManager.getConnection(DB_URL);
		System.err.println("The connection is successfully obtained");
		return connection;
	}
}
