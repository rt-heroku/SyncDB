package com.heroku.syncdbs;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBUtility {
    private static final String[] TYPES = {"TABLE", "VIEW"};
    public static void getTableMetadata(Connection jdbcConnection, String tableNamePattern, String schema, String catalog, boolean isQuoted) throws Exception {
            try {
                DatabaseMetaData meta = jdbcConnection.getMetaData();
                ResultSet rs = null;
                try {
                    if ( (isQuoted && meta.storesMixedCaseQuotedIdentifiers())) {
                        rs = meta.getTables(catalog, schema, tableNamePattern, TYPES);
                    } else if ( (isQuoted && meta.storesUpperCaseQuotedIdentifiers())
                        || (!isQuoted && meta.storesUpperCaseIdentifiers() )) {
                        rs = meta.getTables(
                                catalog,
                                schema,
                                tableNamePattern,
                                TYPES
                            );
                    }
                    else if ( (isQuoted && meta.storesLowerCaseQuotedIdentifiers())
                            || (!isQuoted && meta.storesLowerCaseIdentifiers() )) {
                        rs = meta.getTables( 
                                catalog,
                                schema,
                                tableNamePattern,
                                TYPES 
                            );
                    }
                    else {
                        rs = meta.getTables(catalog, schema, tableNamePattern, TYPES);
                    }

                    while ( rs.next() ) {
                        String tableName = rs.getString("TABLE_NAME");
                        System.out.println("table = " + tableName);
                        
                        String sql = "SELECT * FROM " + tableName.toLowerCase();
                        System.out.println("sql = " + sql);
                        Statement stmt = jdbcConnection.createStatement();
                        ResultSet rsData = stmt.executeQuery(sql);
                        while (rsData.next())
                        	System.out.println(rsData.getString(0));

                    }



                }
                finally {
                    if (rs!=null) rs.close();
                }
            }
            catch (SQLException sqlException) {
                // TODO 
                sqlException.printStackTrace();
            }

    }
    
	private static void testDatabase(Connection conn) throws SQLException {
		Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM TABLE_A");
        while (rs.next()) {
            System.out.println("Read from DB[" + stmt.getConnection().getMetaData().getURL() + "]: " + rs.getString(1));
        }
        
        rs.close();
	}

    
    public static void main(String[] args) {
        Connection jdbcConnection;
        try {
            jdbcConnection = DriverManager.getConnection("jdbc:postgresql://ec2-52-73-169-99.compute-1.amazonaws.com:5432/d3ptaja7fk91s5?user=u8ohh8b179758f&password=p2ch4dj5jkgi216ekj9cedm9lia&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory");
            //getTableMetadata(jdbcConnection, "%", "%", "public", false);
            testDatabase(jdbcConnection);
            
            
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}