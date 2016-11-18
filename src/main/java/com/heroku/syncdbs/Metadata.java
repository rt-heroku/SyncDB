package com.heroku.syncdbs;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class Metadata {

	static Connection connection = null;
	static DatabaseMetaData metadata = null;

	// Static block for initialization
	static {
		try {
			connection = DBConnection.getConnection();
		} catch (SQLException e) {
			System.err.println("There was an error getting the connection: "
					+ e.getMessage());
		}

		try {
			metadata = connection.getMetaData();
		} catch (SQLException e) {
			System.err.println("There was an error getting the metadata: "
					+ e.getMessage());
		}
	}

	/**
	 * Prints in the console the general metadata.
	 * 
	 * @throws SQLException
	 */
	public static void printGeneralMetadata() throws SQLException {
		System.out.println("Database Product Name: "
				+ metadata.getDatabaseProductName());
		System.out.println("Database Product Version: "
				+ metadata.getDatabaseProductVersion());
		System.out.println("Logged User: " + metadata.getUserName());
		System.out.println("JDBC Driver: " + metadata.getDriverName());
		System.out.println("Driver Version: " + metadata.getDriverVersion());
		System.out.println("\n");
	}

	/**
	 * 
	 * @return Arraylist with the table's name
	 * @throws SQLException
	 */
	public static ArrayList<String> getTablesMetadata() throws SQLException {
		String table[] = { "VIEW" };
		ResultSet rs = null;
		ArrayList<String> tables = null;
		// receive the Type of the object in a String array.
		rs = metadata.getTables(null, "public", null, table);
		tables = new ArrayList<String>();
		while (rs.next()) {
			if (!rs.getString("TABLE_NAME").contains("pg_")){
				tables.add(rs.getString("TABLE_NAME"));
				System.out.println("CAT: " + rs.getString("TABLE_CAT"));
				System.out.println("SCHEM: " + rs.getString("TABLE_SCHEM"));
				System.out.println("NAME: " + rs.getString("TABLE_NAME"));
				System.out.println("TYPE: " + rs.getString("TABLE_TYPE"));
			}
		}
		return tables;
	}

	/**
	 * Prints in the console the columns metadata, based in the Arraylist of
	 * tables passed as parameter.
	 * 
	 * @param tables
	 * @throws SQLException
	 */
	public static void getColumnsMetadata(ArrayList<String> tables)
			throws SQLException {
		ResultSet rs = null;
		// Print the columns properties of the actual table
		for (String actualTable : tables) {
			rs = metadata.getColumns(null, null, actualTable, null);
			System.out.println(actualTable.toUpperCase());
			while (rs.next()) {
				System.out.println(rs.getString("COLUMN_NAME") + " "
						+ rs.getString("TYPE_NAME") + " "
						+ rs.getString("COLUMN_SIZE"));
			}
			System.out.println("\n");
		}

	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			printGeneralMetadata();
			// Print all the tables of the database scheme, with their names and
			// structure
			getColumnsMetadata(getTablesMetadata());
		} catch (SQLException e) {
			System.err
					.println("There was an error retrieving the metadata properties: "
							+ e.getMessage());
		}
	}
}