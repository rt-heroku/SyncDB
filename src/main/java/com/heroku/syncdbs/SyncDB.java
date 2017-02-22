package com.heroku.syncdbs;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;

import com.heroku.syncdbs.datamover.DataMover;
import com.heroku.syncdbs.datamover.Database;
import com.heroku.syncdbs.datamover.DatabaseException;
import com.heroku.syncdbs.datamover.PostgreSQL;

public class SyncDB {

	private DataMover mover = new DataMover();
	private Database source = new PostgreSQL();
	private Database target = new PostgreSQL();
	private boolean analyzed = false;
	
	protected boolean isDebugEnabled() {
		return Settings.isDebug();
	}

	public static void main(String[] args) throws Exception {
		SyncDB syncDB = new SyncDB();
		String fromSchema = syncDB.getFromSchema();
		String toSchema = syncDB.getToSchema();
		
		syncDB.copyData(fromSchema, toSchema);
	}

	protected void copyData(String fromSchema, String toSchema) throws Exception {
		try {
			long t1 = System.currentTimeMillis();
			System.out.println("Starting data mover ... " + getCurrentTime());

			connectUsingHerokuVars(getSource(), getTarget());
			
			getMover().setSource(getSource());
			getMover().setTarget(getTarget());

			if (isDebugEnabled()) {
				getMover().printGeneralMetadata(getSource());
				getMover().printGeneralMetadata(getTarget());
			}

			getMover().exportDatabase(fromSchema, toSchema);

			getSource().close();
			getTarget().close();

			System.out.println("Data mover ENDED!" + getCurrentTime());
			long t2 = System.currentTimeMillis();
			System.out.println(" Took " + (t2 - t1) / 1000 + " seconds to run the job!");

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	protected int copyTableChunk(JobMessage jm) throws Exception {
		try {
			validateConnection("target");
			validateConnection("source");
			
			getMover().setJobid(jm.getJobid());
			getMover().setTaskNum(jm.getTasknum());
			
			getMover().copyChunkTable(getToSchema(), jm.getTable(), jm.getOffset(), jm.getChunk());
			
			return getMover().getRowsLoaded();
			
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	protected void copyTable(String fromSchema, String toSchema, TableInfo table) throws Exception {
		try {
			long t1 = System.currentTimeMillis();
			System.out.println("Starting data mover for table [" + table.getFullName() + "] ... " + getCurrentTime());

			connectBothDBs();			

			if (isDebugEnabled()) {
				getMover().printGeneralMetadata(getSource());
				getMover().printGeneralMetadata(getTarget());
			}

			getMover().exportTable(fromSchema, toSchema, table);

			getSource().close();
			getTarget().close();

			System.out.println("Data mover ENDED for table [" + table.getFullName() + "] !" + getCurrentTime());
			long t2 = System.currentTimeMillis();
			System.out.println(" Took " + (t2 - t1) / 1000 + " seconds to run the job!");

		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	public void refreshMaterializedViews(Database db) throws Exception {
		try {
			String views = Settings.getViewsToRefresh();
			String schema = Settings.getMvSchema();

			validateConnection("target");
			validateConnection("source");
			
			if (Settings.useViewInventory())
				refreshViewsSetInViewInventory(db);
			else
				refreshViewsSetInVariable(db, views, schema);
			
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}		
	}

	private void refreshViewsSetInViewInventory(Database db) throws DatabaseException {
		Collection<TableInfo> list = db.getViewstoRefreshFromViewInventory();
		
		for (TableInfo v : list){
			System.out.print("Refreshing MATERIALIZED VIEW " + v.getFullName() + " ... ");
			db.refreshMaterializedView("", v.getFullName());
			db.analyzeTable(v.getFullName());
			System.out.println("Done!");
		}
	}

	private void refreshViewsSetInVariable(Database db, String views, String schema) throws DatabaseException {
		Collection<String> list = Arrays.asList(views.split("\\;"));
		
		for (String v : list){
			String view = schema + ".\"" + v + "\"";
			System.out.print("Refreshing MATERIALIZED VIEW " + view + " ... ");
			db.refreshMaterializedView(schema, v);
			db.analyzeTable(view);
			System.out.println("Done!");
		}
	}
	protected void dropAndRecreateTableInTargetIfExists(TableInfo ti) throws Exception {
		try {
			String toSchema = getToSchema();
			getMover().dropTableIfExistsAndCreateTable(toSchema, ti);
		} catch (DatabaseException e) {
			throw new Exception(e);
		}
	}

	protected void connectUsingJdbcUrls(Database source, Database target) throws SQLException {
		source.connectString(
				"jdbc:postgresql://ec2-54-221-215-139.compute-1.amazonaws.com:5432/d6ln3avkp9rvkh?user=uusopq120p23j&password=pc53e4c364189d3aed8acf921cf769f7d67b8a6d53bb2c20f4242c9200e156f6f&sslmode=require&sslfactory=org.postgresql.ssl.NonValidatingFactory");
		target.connectString(
				"jdbc:postgresql://ec2-107-22-167-159.compute-1.amazonaws.com:5432/d7snsc52h2bm4d?user=u6tp1vvlvd0u1m&password=pcf71fa23ae12377a23522db681191c7620d65a746135e0167aac0ea9976b1cfb&sslmode=require&sslfactory=org.postgresql.ssl.NonValidatingFactory");
	}

	protected void connectUsingHerokuVars(Database source, Database target) throws SQLException {
		connectToSource(source);
		connectToTarget(target);
	}

	public void connectBothDBs() throws Exception{
		try {
			connectUsingHerokuVars(getSource(), getTarget());

			getMover().setSource(getSource());
			getMover().setTarget(getTarget());
		} catch (SQLException e) {
			throw e;
		}
	}

	public void connectBothDBsUsingJDBC() throws Exception{
		try {
			connectUsingJdbcUrls(getSource(), getTarget());

			getMover().setSource(getSource());
			getMover().setTarget(getTarget());
		} catch (SQLException e) {
			throw e;
		}
	}
	public void closeBothConnections() throws Exception{
		try {
			closeConnectionToSource();
			closeConnectionToTarget();
		} catch (SQLException e) {
			throw e;
		}
	}

	protected void connectToSource() throws SQLException{
		connectToSource(getSource());
		getMover().setSource(getSource());
	}
	
	protected void connectToTarget() throws SQLException{
		connectToTarget(getTarget());
		getMover().setTarget(getTarget());
	}
	
	protected void closeConnectionToSource() throws Exception{
		getSource().close();
	}

	protected void closeConnectionToTarget() throws Exception{
		getTarget().close();
	}
	
	public String getFromSchema(){
		return Settings.getDefaultVal(Settings.getTransferFromSchema(), "public");
	}
	
	public String getToSchema(){
		return Settings.getDefaultVal(Settings.getTransferToSchema(), "public");
	}

	public List<TableInfo> analyzeTables(Database db, List<TableInfo> tables) throws Exception {
		for (TableInfo t : tables){
			System.out.print("ANALYZING "  + t.getType() + " " + t.getFullName() + " .... ");
			db.analyzeTable(t.getFullName());
			updateMaxId(db, t);
			if (t.getType().equals("VIEW"))
				System.out.println("\tDone! - maxId = " + t.getMaxid());
			else
				System.out.println("\tDone! - Rows: " + t.getCount() + ",\tmaxId = " + t.getMaxid());
		}
		
		String sql = "UPDATE syncdb.objects_to_transfer o " + 
					"   SET number_of_rows = a.number_of_rows " +
					"  FROM syncdb.all_transferable_objects a " +
					" WHERE     o.schema = a.schema " +
					"       AND o.object_name = a.object_name" + 
					"       AND o.type = a.type";

		db.execute(sql);
		
		System.out.println("ALL OBJECTS ANALYZED!");
		
		analyzed = true;
		
		return getTablesToMoveFromSourceAndGetTheMaxId();
	}

	private void updateMaxId(Database db, TableInfo t) throws Exception{
		String sql = "UPDATE syncdb.objects_to_transfer " + 
					 "   SET maxid = " + t.getMaxid() + 
					 " WHERE schema = '" + t.getSchema() + "'" +
					 "   AND object_name = " + t.getName().replace("\"", "'");
		
		db.execute(sql);
	}

	protected List<TableInfo> getTablesToMoveFromSourceAndGetTheMaxId() throws Exception {
		try{
			validateConnection("source");
		}catch (SQLException e) {
			throw e;
		}
		return mover.getListOfTableNamesAndMaxId(getSource(), getFromSchema(), analyzed);
	}
	
	private void validateConnection(String db) throws SQLException{
		if (db.equals("source"))
			if (source.getConnection().isClosed())
				connectToSource(source);

		if (db.equals("target"))
			if (target.getConnection().isClosed())
				connectToSource(target);
		
	}

	protected void connectToTarget(Database target) throws SQLException {
		String target_var = getTargetDatabase();
		target.connect(target_var);
		System.out.println("Connected to DATABASE: " + target_var);
	}

	public String getTargetDatabase() {
		return Settings.getTargetVar();
	}

	protected void connectToSource(Database source) throws SQLException {
		String source_var = getSourceDatabase();
		source.connect(source_var);
		System.out.println("Connected to DATABASE: " + source_var);
	}

	public String getSourceDatabase() {
		return Settings.getSourceVar();
	}

	protected static String getCurrentTime() {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		return sdf.format(cal.getTime());
	}

	public DataMover getMover() {
		return this.mover;
	}

	public void setMover(DataMover mover) {
		this.mover = mover;
	}

	public Database getSource() {
		return this.source;
	}

	public void setSource(Database source) {
		this.source = source;
	}

	public Database getTarget() {
		return this.target;
	}

	public void setTarget(Database target) {
		this.target = target;
	}


}