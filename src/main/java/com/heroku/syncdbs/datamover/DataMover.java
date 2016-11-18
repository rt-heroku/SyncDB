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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

	/**
	 * Create the specified table. To do this the source database will be
	 * scanned for the table's structure. Then the table will be created in the
	 * target database.
	 * 
	 * @param table
	 *            The name of the table to create.
	 * @throws DatabaseException
	 *             If a database error occurs.
	 */
	public synchronized void createTable(String table) throws DatabaseException {
		String sql;
		// if the table already exists, then drop it
//		if (target.tableExists(table)) {
//			sql = source.generateDrop(table);
//			System.out.println("source: " + sql);
//			target.execute(sql);
//		}

		try{
			sql = source.generateDrop(table);
			System.out.println("source: " + sql);
			target.execute(sql);
		}catch(Exception e){
			System.out.println("Nothing to delete!");
		}
		
		System.out.println("CREATING TABLE " + table);
		// now create the table
		sql = source.generateCreate(table);
		
		System.out.println("DEBUG - " + sql);
		
		target.execute(sql);
	}

	/**
	 * Create all of the tables in the database. This is done by looping over
	 * the list of tables and calling createTable for each.
	 * 
	 * @throws DatabaseException
	 *             If an error occurs.
	 */
	private synchronized void createTables() throws DatabaseException {
		Collection<String> list = source.listTables();
		for (String table : list) {
			try {
				createTable(table);
				tables.add(table);
			} catch (DatabaseException e) {
				e.printStackTrace();
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

	
	
	private synchronized void copyTableFromView(String table) throws DatabaseException {
		String iSql = "INSERT INTO servicesrule_1(id,name,approval_request_type__c,deal_governance_review_level__c,delegated_by__c,delegation_expires__c,delivery_region_s__c,dell_defined_industry__c,dgr_territories__c) VALUES (?,?,?,?,?,?,?,?,?)";
		String sSql = "SELECT id,name,approval_request_type__c,deal_governance_review_level__c,delegated_by__c,delegation_expires__c,delivery_region_s__c,dell_defined_industry__c,dgr_territories__c FROM servicesrule";
		copyFromSelectIntoInsert(sSql, iSql);
	}
	
		private synchronized void copyTable(String table) throws DatabaseException {
		StringBuffer selectSQL = new StringBuffer();
		StringBuffer insertSQL = new StringBuffer();
		StringBuffer values = new StringBuffer();
		Collection<String> columns = source.listColumns(table);
		selectSQL.append("SELECT ");
		insertSQL.append("INSERT INTO ");
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
		selectSQL.append(table);

		insertSQL.append(") VALUES (");
		insertSQL.append(values);
		insertSQL.append(")");

		System.out.println(insertSQL);
		System.out.println(selectSQL);
		
		copyFromSelectIntoInsert(selectSQL.toString(), insertSQL.toString());
	}

		private void copyFromSelectIntoInsert(String selectSQL, String insertSQL)
				throws DatabaseException {
			PreparedStatement statementSrc = null;
			PreparedStatement statementTrg = null;
			ResultSet rs = null;
			
			try {
				statementTrg = target.prepareStatement(insertSQL.toString());
				statementSrc = source.prepareStatement(selectSQL.toString());
				rs = statementSrc.executeQuery();

				int rows = 0;
				System.out.println("Copying data ... ");
				while (rs.next()) {
					rows++;
					
					for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
						
						int type = rs.getMetaData().getColumnType(i);
						
			            if (type == Types.VARCHAR || type == Types.CHAR) {
			            	statementTrg.setString(i, rs.getString(i));
			            	statementTrg.setString(i, rs.getString(i));
			            }else if (type == Types.DATE){
			            	statementTrg.setDate(i, rs.getDate(i));
			            }else if (type == Types.DOUBLE){
			            	statementTrg.setDouble(i, rs.getDouble(i));
			            	
			            }else if (type == Types.DATE){
			            	statementTrg.setDate(i, rs.getDate(i));
			            }else {
			            	statementTrg.setInt(i, rs.getInt(i));
			            }
					}
//				System.out.println(statementTrg.toString());
					statementTrg.execute();
				}
				rs.close();
				statementSrc.close();
				statementTrg.close();
				System.out.println("Rows Inserted: " + rows);
			} catch (SQLException e) {
				throw (new DatabaseException(e));
			} finally {
				try {
					if (rs != null) rs.close();
				} catch (SQLException e) {
					throw (new DatabaseException(e));
				}
				try {
					if (statementSrc != null) statementSrc.close();
				} catch (SQLException e) {
					throw (new DatabaseException(e));
				}
				try {
					if (statementTrg != null) statementTrg.close();
				} catch (SQLException e) {
					throw (new DatabaseException(e));
				}
			}
		}

	private void copyTableData() throws DatabaseException {
		for (String table : tables) {
			copyTable(table);
		}
	}

	public void exportDatabse() throws DatabaseException {
		createTables();
		copyTableData();
	}

	public void copyViewToTableData(String table) throws DatabaseException{
		System.out.println("Deleting data in table " + table);
		truncateTable(table);
		
		System.out.println("About to COPY table data - " + table);
		copyTableFromView(table);
	}
	
	public void copyTableData(String table)throws DatabaseException {
		System.out.println("Deleting data in table " + table);
		truncateTable(table);
		
		System.out.println("About to COPY table data - " + table);
		copyTable(table.toLowerCase());
	}

	private void truncateTable(String table) throws DatabaseException {
		PreparedStatement statementTrg = null;
		try{
			statementTrg = target.prepareStatement("TRUNCATE TABLE " + table);
			statementTrg.execute();
			System.out.println("Table deleted from table: " + table);
		} catch (SQLException e) {
			throw (new DatabaseException(e));
		} finally {
			try {
				if (statementTrg != null) statementTrg.close();
			} catch (SQLException e) {
				throw (new DatabaseException(e));
			}
		}

	}
	
}
