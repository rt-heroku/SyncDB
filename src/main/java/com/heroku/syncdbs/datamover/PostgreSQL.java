
package com.heroku.syncdbs.datamover;

import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgreSQL extends Database {

	public String processType(String type, int size) {
		String usigned = "UNSIGNED";
		int i = type.indexOf(usigned);
		if (i != -1)
			type = type.substring(0, i) + type.substring(i + usigned.length());

		if (type.equalsIgnoreCase("varchar") && (size == 65535))
			type = "TEXT";

		return type.trim();
	}
	
	public void connect (String sysvar) throws SQLException{
    	String url = System.getenv("JDBC_DATABASE_URL") + "";
    	connection = DriverManager.getConnection(url);
	}

}
