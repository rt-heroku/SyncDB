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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.heroku.syncdbs.Settings;
import com.heroku.syncdbs.TableInfo;

/**
 * The Database class is used to provide all of the low-level
 * JDBC services for the Datamover. Database specific 
 * implementations should be handled in derived classes,
 * for example the MySQL class.
 * 
 * @author Jeff Heaton (http://www.heatonresearch.com)
 *
 */
public abstract class Database {
	/*
	 * The database connection
	 */
	protected Connection connection;

    /**
	 * Abstrct method to process a database type. Sometimes database types are
	 * not reported exactly as they need to be for proper syntax. This method
	 * corrects the database type and size.
	 * 
	 * @param type
	 *            The type reported
	 * @param i
	 *            The size of this column
	 * @return The properly formatted type, for this database
	 */
	public abstract String processType(String type, int i);

	public abstract void connect(String var) throws SQLException;
	public abstract void connectString(String jdbc) throws SQLException;
	
	
	protected boolean isDebugEnabled(){
    	return Settings.isDebug();
    }
   /**
	 * Open a connection to the database.
	 * 
	 * @param driver
	 *            The database driver to use.
	 * @param url
	 *            The datbase connection URL to use.
	 * @throws DatabaseException
	 *             Thrown if an error occurs while connecting.
	 */
	public void connect(String driver, String url) throws DatabaseException {
		try {
			Class.forName(driver).newInstance();
			connection = DriverManager.getConnection(url);
		} catch (InstantiationException e) {
			throw new DatabaseException(e);
		} catch (IllegalAccessException e) {
			throw new DatabaseException(e);
		} catch (ClassNotFoundException e) {
			throw new DatabaseException(e);
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}
  
  /**
	 * Called to close the database.
	 * 
	 * @throws DatabaseException
	 *             Thrown if the connection cannot be closed.
	 */
	public void close() throws DatabaseException {
		try {
			synchronized ( connection ) {
				if (connection!=null) connection.close();
			}
		} catch (SQLException e) {
			throw (new DatabaseException(e));
		}
	}

	/**
	 * Check to see if the specified type is numeric.
	 * 
	 * @param type
	 *            The type to check.
	 * @return Returns true if the type is numeric.
	 */
	public boolean isNumeric(int type) {
		if (type == java.sql.Types.BIGINT || type == java.sql.Types.DECIMAL
				|| type == java.sql.Types.DOUBLE
				|| type == java.sql.Types.FLOAT
				|| type == java.sql.Types.INTEGER
				|| type == java.sql.Types.NUMERIC
				|| type == java.sql.Types.SMALLINT
				|| type == java.sql.Types.TINYINT)
			return true;
		else
			return false;

	}

	/**
	 * Generate the DROP statement for a table.
	 * 
	 * @param table
	 *            The name of the table to drop.
	 * @return The SQL to drop a table.
	 */
	public String generateDropTableSQLStatement(String schema, String table) {
		StringBuffer result = new StringBuffer();
		result.append("DROP TABLE ");
		result.append(schema + ".");
		result.append(table);
		//result.append(";\n");
		return result.toString();
	}

	/**
	 * Generate the create statement to create the specified table.
	 * 
	 * @param table
	 *            The table to generate a create statement for.
	 * @return The create table statement.
	 * @throws DatabaseException
	 *             If a database error occurs.
	 */
	public String generateCreateTableSQLStatement(String sTo, TableInfo table) throws DatabaseException {
		StringBuffer result = new StringBuffer();

		ResultSetMetaData md = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		int maxId = table.getMaxid();
		
		synchronized ( connection ) {
			try {
				StringBuffer sql = new StringBuffer();
				
				//Only 1 row to get the definition
				sql.append("SELECT * FROM ");
				sql.append(table.getFullName());
				
				if (maxId > 0)
					sql.append(" WHERE id = " + maxId);
				else
					sql.append(" LIMIT 1");
				
				if (isDebugEnabled())
					System.out.println(sql.toString());

				stmt = connection.createStatement();
				stmt.setFetchSize(1);
				rs = stmt.executeQuery(sql.toString());
				md = rs.getMetaData();
	
				result.append("CREATE TABLE ");
				result.append(sTo + "." + table.getName());
				result.append(" ( ");
	
				boolean idColumnFound = false;
				
				for (int i = 1; i <= md.getColumnCount(); i++) {
					if (i != 1)
						result.append(',');
					result.append("\"" + md.getColumnName(i) + "\"");
					result.append(' ');
	
					if (!idColumnFound)
						idColumnFound = md.getColumnName(i).equals("id");
					
					String type = processType(md.getColumnTypeName(i), md
							.getPrecision(i));
					result.append(type);
					if (md.isNullable(i) == ResultSetMetaData.columnNoNulls) {
						result.append("NOT NULL ");
					} else {
						// result.append("NULL ");
					}
					if (md.isAutoIncrement(i))
						result.append(" auto_increment");
				}
	
				DatabaseMetaData dbm = connection.getMetaData();
				ResultSet primary = dbm.getPrimaryKeys(null, table.getSchema(), table.getName());
				boolean first = true;
				boolean foundPK = false;
				while (primary.next()) {
					foundPK = true;
					if (first) {
						first = false;
						result.append(',');
						result.append("PRIMARY KEY(");
					} else
						result.append(",");
	
					result.append(primary.getString("COLUMN_NAME"));
				}
				
				//since it is reading VIEWs and they don't have PK, we know that the id field has to be
				if ((!foundPK) && (idColumnFound)){
					result.append(",PRIMARY KEY(id");
					first = false;
				}
	
				if (!first)
					result.append(')');
	
				result.append(" ); ");
	
				rs.close();
				stmt.close();
			} catch (SQLException e) {
				throw (new DatabaseException(e));
			} finally {
				try {
					if (rs != null)
						rs.close();
				} catch (SQLException e) {
					throw (new DatabaseException(e));
				}
				try {
					if (stmt != null)
						stmt.close();
				} catch (SQLException e) {
					throw (new DatabaseException(e));
				}
			}
		}
		
		return result.toString();
	}

	/**
	 * Execute a INSERT, DELETE, UPDATE, or other statement that does not return
	 * a ResultSet.
	 * 
	 * @param sql
	 *            The query to execute.
	 * @throws DatabaseException
	 *             If a database error occurs.
	 */
	public void execute(String sql) throws DatabaseException {
		Statement stmt = null;
		try {
			if (isDebugEnabled())
				System.out.println(sql);
			stmt = connection.createStatement();
			stmt.execute(sql);
			stmt.close();
		} catch (SQLException e) {
			System.out.println("Syntax error? in " + sql);
			throw (new DatabaseException(e));
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Get a list of all tables in the database.
	 * 
	 * @return A list of all tables in the database.
	 * @throws DatabaseException
	 *             If a database error occurs.
	 */
	public Collection<TableInfo> listTables(String schema) throws DatabaseException {
		Collection<TableInfo> result = new ArrayList<TableInfo>();
		ResultSet rs = null;

		try {
			DatabaseMetaData dbm;
			dbm = connection.getMetaData();

			String types[] = { "VIEW", "MATERIALIZED VIEW", "TABLE" };
			rs = dbm.getTables(null, schema, null, types);
			while (rs.next()) {
				String str = "\"" + rs.getString("TABLE_NAME") + "\"";
				if (!str.startsWith("pg_")){
					String type = rs.getString("TABLE_TYPE");
					TableInfo t = new TableInfo(schema,str,type);
					//if (isDebugEnabled())
						System.out.println("Found [" + type + "] - NAMED = " + schema + "." + str);
					result.add(t);
				}
			}
			rs.close();
		} catch (SQLException e) {
			throw (new DatabaseException(e));
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
				}
			}
		}

		return result;
	}
	public Collection<TableInfo> getListTablesFromInventory(boolean analyzed) throws DatabaseException{
		List<TableInfo> lti = new ArrayList<TableInfo>();
		String sql;
		PreparedStatement statementSrc = null;
		ResultSet rs = null;
		try {

			sql = "SELECT \"schema\", object_name, \"type\", \"number_of_rows\", \"refresh\", \"analyze\", \"maxid\" FROM syncdb.objects_to_transfer WHERE transfer = true";
			//sql = "SELECT \"schema\", object_name, \"type\", \"number_of_rows\", \"refresh\", \"analyze\", \"maxid\" FROM syncdb.rt_object_test WHERE transfer = true";
			statementSrc = connection.prepareStatement(sql);
			rs = statementSrc.executeQuery();

			while (rs.next()){
				TableInfo t = new TableInfo();
				t.setSchema(rs.getString(1));
				t.setName("\"" + rs.getString(2) + "\"");
				t.setType(rs.getString(3));
				t.setCount(rs.getInt(4));
				t.setRefresh(rs.getBoolean(5));
				t.setAnalyze(rs.getBoolean(6));
				t.setMaxid(rs.getInt(7));
				lti.add(t);
				if (!analyzed)
					System.out.println("Found [" + t.getType() + "] - NAMED = " + t.getFullName());
			}
			
		} catch (Exception e) {
			System.out.println("Error while retrieving items to sync - " + e.getMessage());
			throw new DatabaseException(e);
		} finally {
			try{
				rs.close();
				statementSrc.close();
			} catch (Exception e){
				System.out.println(e.getMessage());
			}
		}
		return lti;
	}

	public Collection<TableInfo> getViewstoRefreshFromViewInventory() throws DatabaseException{
		List<TableInfo> lti = new ArrayList<TableInfo>();
		String sql;
		PreparedStatement statementSrc = null;
		ResultSet rs = null;
		try {

			sql = "SELECT \"schema\", object_name, \"type\", \"number_of_rows\", \"refresh\", \"analyze\", \"maxid\" FROM syncdb.objects_to_transfer WHERE refresh = true";
			statementSrc = connection.prepareStatement(sql);
			rs = statementSrc.executeQuery();

			while (rs.next()){
				TableInfo t = new TableInfo();
				t.setSchema(rs.getString(1));
				t.setName("\"" + rs.getString(2) + "\"");
				t.setType(rs.getString(3));
				t.setCount(rs.getInt(4));
				t.setRefresh(rs.getBoolean(5));
				t.setAnalyze(rs.getBoolean(6));
				t.setMaxid(rs.getInt(7));
				lti.add(t);
			}
			
		} catch (Exception e) {
			System.out.println("Error while retrieving items to sync - " + e.getMessage());
			throw new DatabaseException(e);
		} finally {
			try{
				rs.close();
				statementSrc.close();
			} catch (Exception e){
				
			}
		}
		return lti;
	}

	/**
	 * Determine if a table exists.
	 * 
	 * @param table
	 *            The name of the table.
	 * @return True if the table exists.
	 * @throws DatabaseException
	 *             A database error occured.
	 */
	public boolean tableExists(String table, String schema) throws DatabaseException {
		boolean result = false;
		ResultSet rs = null;

		try {
			DatabaseMetaData dbm;
			dbm = connection.getMetaData();
			
			String types[] = { "TABLE" };
			rs = dbm.getTables(null, schema, table.replace("\"", ""), types);
			result = rs.next();
			rs.close();
		} catch (SQLException e) {
			throw (new DatabaseException(e));
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
				}
			}
		}
		return result;
	}

	/**
	 * Get a list of all of the columns on a table.
	 * 
	 * @param table
	 *            The table to check.
	 * @return A list of all of the columns.
	 * @throws DatabaseException
	 *             If a database error occurs.
	 */
	public Collection<String> listColumns(String table, String schema)
			throws DatabaseException {
		Collection<String> result = new ArrayList<String>();
		ResultSet rs = null;

		try {
			DatabaseMetaData dbm;
			dbm = connection.getMetaData();
			rs = dbm.getColumns(null, schema, table.replace("\"", ""), null);
			while (rs.next()) {
				result.add(rs.getString("COLUMN_NAME"));
			}
			rs.close();
		} catch (SQLException e) {
			throw new DatabaseException(e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
				}
			}
		}
		return result;
	}

	/**
	 * Create a prepared statement.
	 * 
	 * 
	 * @param sql
	 *            The SQL of the prepared statement.
	 * @return The PreparedStatement that was created.
	 * @throws DatabaseException
	 *             If a database error occurs.
	 */
	public PreparedStatement prepareStatement(String sql)
			throws DatabaseException {
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(sql);
		} catch (SQLException e) {
			throw (new DatabaseException(e));
		}
		return statement;
	}

	public PreparedStatement prepareForwardStatement(String sql)
			throws DatabaseException {
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
		} catch (SQLException e) {
			throw (new DatabaseException(e));
		}
		return statement;
	}

	/**
	 * @return the connection
	 */
	public Connection getConnection() {
		return connection;
	}

	public void refreshMaterializedView(String schema, String v) throws DatabaseException {
		String view = schema.equals("") ? v : schema + ".\"" + v + "\"";
		String sql = "REFRESH MATERIALIZED VIEW " + view;
		execute(sql);
	}

	public void analyzeTable(String table) throws DatabaseException{
		String sql = "ANALYZE " + table;
		execute(sql);
	}


}
