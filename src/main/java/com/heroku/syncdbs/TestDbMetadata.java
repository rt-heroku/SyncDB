package com.heroku.syncdbs;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TestDbMetadata {

	public static void main(String[] args) throws Exception {
		TestDbMetadata t = new TestDbMetadata();
		t.run();
	}

	private void run() throws Exception {
		SyncDB s = new SyncDB();

		s.connectToSource();
		Connection conn = s.getSource().getConnection();
		DatabaseMetaData databaseMetaData = conn.getMetaData();
		
		String schema = s.getFromSchema();
		String table = "";
		
		printDbInfo(databaseMetaData);		
		
		List<Table> tables = listTables(databaseMetaData, schema);

		List<Column> columns = listColumnsInTable(databaseMetaData, schema, new Table(schema,table,"VIEW"));

		
		listColumnsInAllTables(databaseMetaData, schema, tables);

	}

	private void listColumnsInAllTables(DatabaseMetaData databaseMetaData, String schema, List<Table> tables) throws SQLException {
		for (Table t : tables){
			List<Column> columns = listColumnsInTable(databaseMetaData, schema, t);
			t.setColumns(columns);
		}
	}
	
	private void printDbInfo(DatabaseMetaData databaseMetaData) throws SQLException {
		System.out.println("majorVersion =" + databaseMetaData.getDatabaseMajorVersion());
		System.out.println("minorVersion =" + databaseMetaData.getDatabaseMinorVersion());
		System.out.println("productName =" + databaseMetaData.getDatabaseProductName());
		System.out.println("productVersion =" + databaseMetaData.getDatabaseProductVersion());
		System.out.println("driverMajorVersion =" + databaseMetaData.getDriverMajorVersion());
		System.out.println("driverMinorVersion =" + databaseMetaData.getDriverMinorVersion());
		System.out.println("supportsGetGeneratedKeys =" + databaseMetaData.supportsGetGeneratedKeys());
		System.out.println("supportsGroupBy =" + databaseMetaData.supportsGroupBy());
		System.out.println("supportsOuterJoins =" + databaseMetaData.supportsOuterJoins());
		System.out.println("storesLowerCaseIdentifiers =" + databaseMetaData.storesLowerCaseIdentifiers());
		System.out.println("storesUpperCaseIdentifiers =" + databaseMetaData.storesUpperCaseIdentifiers());
		System.out.println("storesMixedCaseIdentifiers =" + databaseMetaData.storesMixedCaseIdentifiers());
		System.out.println("storesMixedCaseQuotedIdentifiers =" + databaseMetaData.storesMixedCaseQuotedIdentifiers());
		System.out.println("storesLowerCaseQuotedIdentifiers =" + databaseMetaData.storesLowerCaseQuotedIdentifiers());
		System.out.println("storesUpperCaseQuotedIdentifiers =" + databaseMetaData.storesUpperCaseQuotedIdentifiers());
		System.out.println("getExtraNameCharacters =" + databaseMetaData.getExtraNameCharacters());
	}

	private List<Column> listColumnsInTable(DatabaseMetaData databaseMetaData, String schema, Table table)throws SQLException {
        ResultSet rs = null;
	        List<Column> cols = new ArrayList<Column>();
            rs = databaseMetaData.getColumns(null, schema, table.getName(), null);
            while (rs.next()) {
            	Column col = new Column(rs.getString("COLUMN_NAME") , rs.getString("TYPE_NAME") , rs.getString("COLUMN_SIZE"));
				cols.add(col);
            }
            table.setColumns(cols);
		return cols;
	}

	private List<Table> listTables(DatabaseMetaData databaseMetaData, String schema) throws SQLException {
        String table[] = { "TABLE", "VIEW" };
        ResultSet result = databaseMetaData.getTables(
			    "", schema, null, table);
        
		List<Table> tables = new ArrayList<Table>();
			while(result.next()) {
				Table t = new Table(result.getString(2), result.getString(3), result.getString(4));
			    tables.add(t);
			}
			return tables;
	}
	
	/*
	int numCols = rsmd.getColumnCount();
    for (int i = 1; i <= numCols; i++) {
      if (i > 1)
        System.out.print(", ");
      System.out.print(rsmd.getColumnLabel(i));
    }
    System.out.println("");
    while (rs.next()) {
      for (int i = 1; i <= numCols; i++) {
        if (i > 1)
          System.out.print(", ");
        System.out.print(rs.getString(i));
      }
      System.out.println("");
    }
	 */
	public class Table {
		private String name;
		private String schema;
		private String type;
		
		private List<Column> columns;
		
		public Table(String schema, String name, String type) {
			this.name = name;
			this.schema = schema;
			this.type = type;
		}

		@Override
		public String toString(){
			return name;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public List<Column> getColumns() {
			return columns;
		}

		public void setColumns(List<Column> columns) {
			this.columns = columns;
		}

		public String getSchema() {
			return schema;
		}

		public void setSchema(String schema) {
			this.schema = schema;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}
	}
	
	public class Column {
		private String name;
		private String type;
		private String size;
		
		public Column(String name, String type, String size) {
			this.name = name;
			this.type = type;
			this.size = size;
		}

		@Override
		public String toString(){
			return name;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getSize() {
			return size;
		}

		public void setSize(String size) {
			this.size = size;
		}

	}
	
}
