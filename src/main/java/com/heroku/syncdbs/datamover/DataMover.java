/*
 * Copyright 2009 Carnegie Mellon University
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.heroku.syncdbs.datamover;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.heroku.syncdbs.JobLoggerHelper;

/**
 * Generic data mover class. This class is designed to move data from one
 * database to another. To do this, first the tables are created in the target
 * database, then all data from the source database is copied.
 * 
 * @author Jeff Heaton (http://www.heatonresearch.com)
 * 
 */
public class DataMover {
	/**
	 * The source database.
	 */
	private Database source;

	/**
	 * The target database.
	 */
	private Database target;

	private int rowsLoaded = 0;
	private String jobid;
	private int taskNum;
	
	/**
	 * The list of tables, from the source database.
	 */
	private List<String> tables = new ArrayList<String>();

	public Database getSource() {
		return source;
	}

	public void setSource(Database source) {
		this.source = source;
	}

	public Database getTarget() {
		return target;
	}

	public void setTarget(Database target) {
		this.target = target;
	}

	public void printGeneralMetadata(Database db) throws SQLException {
		DatabaseMetaData metadata = db.getConnection().getMetaData();
		System.out.println("Database Product Name: " + metadata.getDatabaseProductName());
		System.out.println("Database Product Version: " + metadata.getDatabaseProductVersion());
		System.out.println("Logged User: " + metadata.getUserName());
		System.out.println("JDBC Driver: " + metadata.getDriverName());
		System.out.println("Driver Version: " + metadata.getDriverVersion());
		System.out.println("\n");
	}

	protected boolean isDebugEnabled() {
		String ret = System.getenv("DEBUG") + "";
		return ret.equals("TRUE");
	}

	public String getFromSchema(){
		String schema = "public";
		if (System.getenv("TRANSFER_FROM_SCHEMA") != null)
			schema = System.getenv("TRANSFER_FROM_SCHEMA");
		return schema;
	}
	
	public String getToSchema(){
		String schema = "public";
		if (System.getenv("TRANSFER_TO_SCHEMA") != null)
			schema = System.getenv("TRANSFER_TO_SCHEMA");
		return schema;
	}
	/**
	 * Create the specified table. To do this the source database will be
	 * scanned for the table's structure. Then the table will be created in the
	 * target database.
	 * 
	 * @param fromSchema
	 * 				Schema From where it is reading the data
	 * @param toSchema
	 * 				Schema where it will write data
	 * @param table
	 *            The name of the table to create.
	 * @throws DatabaseException
	 *             If a database error occurs.
	 */
	public synchronized void dropTableIfExistsAndCreateTable(String fromSchema, String toSchema, String table, int maxId) throws DatabaseException {
		String sql;

		try {
			sql = target.generateDropTableSQLStatement(toSchema, table);
			target.execute(sql);
			if (isDebugEnabled()){
				System.out.println("TABLE DROPPED: " + table);
				System.out.println("DEBUG - " + sql);
			}

		} catch (Exception e) {
			System.out.println("Error while deleting table - " + e.getMessage());
		}
		sql = source.generateCreateTableSQLStatement(fromSchema, toSchema, table, maxId);
		if (isDebugEnabled()){
			System.out.println("CREATING TABLE " + table);
			System.out.println("DEBUG - execute in target: " + sql);
		}
		target.execute(sql);
	}

	public synchronized int getTableMaxId(Database db, String table) throws DatabaseException {
		String sql;
		int count = 0;
		try {

			sql = "SELECT MAX(id) FROM " + table;
			PreparedStatement statementSrc = db.prepareStatement(sql);
			ResultSet rs = statementSrc.executeQuery();

			if (rs.next())
				count = rs.getInt(1);
			
			rs.close();
			statementSrc.close();

		} catch (Exception e) {
			System.out.println("Error while counting rows in table [" + table + "] - " + e.getMessage());
			throw new DatabaseException(e);
		}
		return count;
	}

	public synchronized Map<String, Integer> getListOfTableNamesAndMaxId(Database db, String schema) throws Exception {
		Map<String, Integer> m = new HashMap<String, Integer>();
		Collection<String> list = db.listTables(schema);
		for (String table : list) {
			try {
				if (!table.startsWith("pg_")) {
					System.out.println("table to get max id = " + schema + "." + table);
					int count = getTableMaxId(db, schema + "." + table);
					m.put(table, count);
				}
			} catch (DatabaseException e) {
				e.printStackTrace();
				throw new Exception(e);
			}
		}
		return m;
	}

	/**
	 * Create all of the tables in the database. This is done by looping over
	 * the list of tables and calling createTable for each.
	 * 
	 * @throws DatabaseException
	 *             If an error occurs.
	 */
	private synchronized void dropAllTablesIfExistsAndCreateAllTables(String fromSchema, String toSchema) throws DatabaseException {
		Collection<String> list = source.listTables(fromSchema);
		for (String table : list) {
			try {
				if (!table.startsWith("pg_")) {
					dropTableIfExistsAndCreateTable(fromSchema, toSchema, table, 0);
					tables.add(table);
				}
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}
	}

	protected synchronized void debugSourceViews(String schema) throws DatabaseException {
		Collection<String> list = source.listTables(schema);
		for (String table : list) {
			System.out.println("Found Table/View:" + table);

			Collection<String> columns = source.listColumns(schema, table);

			for (String column : columns) {
				System.out.println("\tColumn name: " + column);
			}
		}
	}

	protected synchronized void debugTargetTables(String schema) throws DatabaseException {
		Collection<String> list = target.listTables(schema);
		for (String table : list) {
			System.out.println("Found Table in target: " + table);

			Collection<String> columns = source.listColumns(schema, table);

			for (String column : columns) {
				System.out.println("\tColumn name: " + column);
			}
		}
	}

	/**
	 * Copy the data from one table to another. To do this both a SELECT and
	 * INSERT statement must be created.
	 * 
	 * @param table
	 *            The table to copy.
	 * @throws DatabaseException
	 *             If a database error occurs.
	 */

	private synchronized void createSelectAndInsertStatementsAndCopyTableData(String fromSchema, String toSchema, String table, int offset, int limit) throws DatabaseException {
		StringBuffer selectSQL = new StringBuffer();
		StringBuffer insertSQL = new StringBuffer();
		StringBuffer values = new StringBuffer();
		Collection<String> columns = source.listColumns(fromSchema, table);
		selectSQL.append("SELECT ");
		insertSQL.append("INSERT INTO ");
		insertSQL.append(toSchema + ".");
		insertSQL.append(table.toLowerCase());
		insertSQL.append("(");

		boolean first = true;
		for (String column : columns) {
			if (!first) {
				selectSQL.append(",");
				insertSQL.append(",");
				values.append(",");
			} else
				first = false;

			selectSQL.append(column.toLowerCase());
			insertSQL.append(column.toLowerCase());
			values.append("?");
		}
		selectSQL.append(" FROM ");
		selectSQL.append(fromSchema + ".");
		selectSQL.append(table);

		insertSQL.append(") VALUES (");
		insertSQL.append(values);
		insertSQL.append(")");

		if (isDebugEnabled()) {
			System.out.println("DEBUG SQL: " + insertSQL);
			System.out.println("DEBUG SQL: " + selectSQL);
		}

		if ((offset + limit) > 0)
			selectSQL.append(" WHERE id >= " + offset + " AND id < " + (offset + limit));

		copyDataFromSelectIntoInsert(selectSQL.toString(), insertSQL.toString(), table);
	}

	private void copyDataFromSelectIntoInsert(String selectSQL, String insertSQL, String table) throws DatabaseException {
		PreparedStatement statementTrg = null;
		PreparedStatement statementSrc = null;
		ResultSet rs = null;
		int type = 0;
		boolean hasCommited = false;
		try {
			target.getConnection().setAutoCommit(false);
			statementSrc = source.prepareForwardStatement(selectSQL.toString());
			statementSrc.setFetchSize(5000);
			rs = statementSrc.executeQuery();
			System.out.println(insertSQL.toString());
			int rows = 0;
			if (isDebugEnabled()) 
				System.out.println("Copying data TABLE [" + table + "] ... ");

			while (rs.next()) {
				hasCommited = false;
				statementTrg = target.prepareStatement(insertSQL.toString());
				rows++;

				type = insertRow(statementTrg, rs, type);

				if ((rows % 10000) == 0) {
					target.getConnection().commit();
					hasCommited = true;
				}
				if ((rows % 5000) == 0){
					JobLoggerHelper.logTask(getSource(), getJobid(), getTaskNum() ,table, rows);
				}
				if ((rows % 100000) == 0){
					//maybe log every x amount of rows ... 10k? 1k?
					System.out.println("TABLE [" + table + "] Rows -- " + rows);
				}
				statementTrg.close();
			}
			if (!hasCommited)
				target.getConnection().commit();
			
			setRowsLoaded(rows);
			
			if (rows > 0){
				JobLoggerHelper.logTask(getSource(), getJobid(), getTaskNum() ,table, rows);
				System.out.println("TABLE [" + table + "] Rows -- " + rows);
			}
		} catch (SQLException e) {
			System.err.println("column type = " + getSqlTypeName(type));
			try {
				target.getConnection().rollback();
			} catch (SQLException de) {
				System.err.println("Unable to rollback last transaction!");
			}
			throw (new DatabaseException(e));
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				throw (new DatabaseException(e));
			}
			try {
				if (statementSrc != null)
					statementSrc.close();
			} catch (SQLException e) {
				throw (new DatabaseException(e));
			}
			try {
				if (statementTrg != null)
					statementTrg.close();
			} catch (SQLException e) {
				throw (new DatabaseException(e));
			}
		}
	}

	private int insertRow(PreparedStatement statementTrg, ResultSet rs, int type) throws SQLException {
		for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {

			type = rs.getMetaData().getColumnType(i);

			if (type == Types.VARCHAR || type == Types.CHAR)
				statementTrg.setString(i, rs.getString(i));
			else if (type == Types.DATE)
				statementTrg.setDate(i, rs.getDate(i));
			else if (type == Types.DOUBLE)
				statementTrg.setDouble(i, rs.getDouble(i));
			else if (type == Types.DATE)
				statementTrg.setDate(i, rs.getDate(i));
			else if (type == Types.TIMESTAMP)
				statementTrg.setTimestamp(i, rs.getTimestamp(i));
			else if (type == Types.TIME)
				statementTrg.setTime(i, rs.getTime(i));
			else if (type == Types.TIME_WITH_TIMEZONE)
				statementTrg.setTime(i, rs.getTime(i));
			else if (type == Types.TIMESTAMP_WITH_TIMEZONE)
				statementTrg.setTimestamp(i, rs.getTimestamp(i));
			else if (type == Types.FLOAT)
				statementTrg.setFloat(i, rs.getFloat(i));
			else if (type == Types.BIT || type == Types.BOOLEAN)
				statementTrg.setBoolean(i, rs.getBoolean(i));
			else if (type == Types.NULL)
				statementTrg.setNull(i, 0);
			else
				statementTrg.setInt(i, rs.getInt(i));
		}

		//if (isDebugEnabled())
			System.out.println(statementTrg.toString());
		statementTrg.execute();
		return type;
	}

	private void copyTableData(String sFrom,String sTo) throws DatabaseException {
		for (String table : tables) {
			long t1 = System.currentTimeMillis();
			createSelectAndInsertStatementsAndCopyTableData(sFrom, sTo, table, 0, 0);
			System.out
					.println("Table " + table + " copied in " + (System.currentTimeMillis() - t1) / 1000 + " seconds");
		}
	}

	public void exportDatabase(String fromSchema, String toSchema) throws DatabaseException {
		if (isDebugEnabled()) {
			debugSourceViews(fromSchema);
			debugTargetTables(toSchema);
		}
		dropAllTablesIfExistsAndCreateAllTables(fromSchema, toSchema);
		copyTableData(fromSchema, toSchema);
	}

	public void copyChunkTable(String fromSchema, String toSchema, String table, int offset, int limit) throws DatabaseException{
//		long t1 = System.currentTimeMillis();
		setRowsLoaded(0);
		createSelectAndInsertStatementsAndCopyTableData(fromSchema, toSchema, table, offset, limit);
//		System.out.println("Table " + table + " chunk size[" + limit + "] copied in " + (System.currentTimeMillis() - t1) / 1000 + " seconds");
	}
	public void exportDatabase(String fromSchema, String toSchema, String table) throws DatabaseException {
		dropTableIfExistsAndCreateTable(fromSchema, toSchema, table, 0);
		long t1 = System.currentTimeMillis();
		createSelectAndInsertStatementsAndCopyTableData(fromSchema, toSchema, table, 0, 0);
		System.out.println("Table " + table + " copied in " + (System.currentTimeMillis() - t1) / 1000 + " seconds");
	}

	public void copyTableData(String fromSchema, String toSchema, String table) throws DatabaseException {
		System.out.println("Deleting data in table " + table);
		truncateTable(table);

		System.out.println("About to COPY table data - " + table);
		createSelectAndInsertStatementsAndCopyTableData(fromSchema, toSchema, table.toLowerCase(), 0, 0);
	}

	private void truncateTable(String table) throws DatabaseException {
		PreparedStatement statementTrg = null;
		try {
			statementTrg = target.prepareStatement("TRUNCATE TABLE " + table);
			statementTrg.execute();
			System.out.println("Table deleted from table: " + table);
		} catch (SQLException e) {
			throw (new DatabaseException(e));
		} finally {
			try {
				if (statementTrg != null)
					statementTrg.close();
			} catch (SQLException e) {
				throw (new DatabaseException(e));
			}
		}

	}

	public static String getSqlTypeName(int type) {
		switch (type) {
		case Types.BIT:
			return "BIT";
		case Types.TINYINT:
			return "TINYINT";
		case Types.SMALLINT:
			return "SMALLINT";
		case Types.INTEGER:
			return "INTEGER";
		case Types.BIGINT:
			return "BIGINT";
		case Types.FLOAT:
			return "FLOAT";
		case Types.REAL:
			return "REAL";
		case Types.DOUBLE:
			return "DOUBLE";
		case Types.NUMERIC:
			return "NUMERIC";
		case Types.DECIMAL:
			return "DECIMAL";
		case Types.CHAR:
			return "CHAR";
		case Types.VARCHAR:
			return "VARCHAR";
		case Types.LONGVARCHAR:
			return "LONGVARCHAR";
		case Types.DATE:
			return "DATE";
		case Types.TIME:
			return "TIME";
		case Types.TIMESTAMP:
			return "TIMESTAMP";
		case Types.BINARY:
			return "BINARY";
		case Types.VARBINARY:
			return "VARBINARY";
		case Types.LONGVARBINARY:
			return "LONGVARBINARY";
		case Types.NULL:
			return "NULL";
		case Types.OTHER:
			return "OTHER";
		case Types.JAVA_OBJECT:
			return "JAVA_OBJECT";
		case Types.DISTINCT:
			return "DISTINCT";
		case Types.STRUCT:
			return "STRUCT";
		case Types.ARRAY:
			return "ARRAY";
		case Types.BLOB:
			return "BLOB";
		case Types.CLOB:
			return "CLOB";
		case Types.REF:
			return "REF";
		case Types.DATALINK:
			return "DATALINK";
		case Types.BOOLEAN:
			return "BOOLEAN";
		case Types.ROWID:
			return "ROWID";
		case Types.NCHAR:
			return "NCHAR";
		case Types.NVARCHAR:
			return "NVARCHAR";
		case Types.LONGNVARCHAR:
			return "LONGNVARCHAR";
		case Types.NCLOB:
			return "NCLOB";
		case Types.SQLXML:
			return "SQLXML";
		}

		return "?";
	}

	public int getRowsLoaded() {
		return rowsLoaded;
	}

	public void setRowsLoaded(int rowsLoaded) {
		this.rowsLoaded = rowsLoaded;
	}

	public int getTaskNum() {
		return taskNum;
	}

	public void setTaskNum(int taskNum) {
		this.taskNum = taskNum;
	}

	public String getJobid() {
		return jobid;
	}

	public void setJobid(String jobid) {
		this.jobid = jobid;
	}

}