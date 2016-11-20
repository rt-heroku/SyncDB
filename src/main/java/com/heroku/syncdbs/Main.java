package com.heroku.syncdbs;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.heroku.syncdbs.datamover.DataMover;
import com.heroku.syncdbs.datamover.Database;
import com.heroku.syncdbs.datamover.PostgreSQL;

public class Main {
	final static Logger logger = LoggerFactory.getLogger(Main.class);
    
    public static String SOURCE_VAR="HEROKU_POSTGRESQL_GRAY_JDBC_URL";
    public static String TARGET_VAR="HEROKU_POSTGRESQL_JADE_JDBC_URL";
    
    protected static boolean isDebugEnabled(){
    	String ret = System.getenv("DEBUG") + "";
    	return ret.equals("TRUE");
    }
    
    public static void main(String[] args) throws Exception {
        try {
        	Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

            scheduler.start();

            JobDetail jobDetail = newJob(CopyDatabaseJob.class).build();
            
            CronTrigger trigger = newTrigger()
            	      .withIdentity("trigger1", "group1")
            	      .startNow()
            	      .withSchedule(cronSchedule("0 * 22 * * ?"))            
            	      .build();
            
            logger.info(trigger.getExpressionSummary());
            
            scheduler.scheduleJob(jobDetail, trigger);

        } catch (SchedulerException se) {
            se.printStackTrace();
        }

    }

    protected static void connectUsingJdbcUrls(Database source, Database target) throws SQLException {
		source.connectString("jdbc:postgresql://ec2-52-73-169-99.compute-1.amazonaws.com:5432/d3ptaja7fk91s5?user=u8ohh8b179758f&password=p2ch4dj5jkgi216ekj9cedm9lia&sslmode=require&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory");
		target.connectString("jdbc:postgresql://ec2-52-200-41-184.compute-1.amazonaws.com:5432/d9mgkh21nofekg?user=uegso4e2g4jqof&password=p991t3gs4ehj3ublia03ssn3jgs&sslmode=require&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory");
	}

	protected static void connectUsingHerokuVars(Database source, Database target) throws SQLException {
		source.connect(SOURCE_VAR);
		System.out.println("Connected to DATABSE: " + SOURCE_VAR);

		target.connect(TARGET_VAR);
		System.out.println("Connected to DATABSE: " + TARGET_VAR);
	}
	
	protected static String getCurrentTime(){
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(cal.getTime());
	}
	 public static class CopyDatabaseJob implements Job {

		 protected static void doJob() throws Exception{
				try {
					long t1 = System.currentTimeMillis();
					System.out.println("Starting data mover ... " + getCurrentTime());
					DataMover mover = new DataMover();
					Database source = new PostgreSQL();
					Database target = new PostgreSQL();

					connectUsingHerokuVars(source, target);
					
					mover.setSource(source);
					mover.setTarget(target);
					
					if (isDebugEnabled()){
						mover.printGeneralMetadata(source);
						mover.printGeneralMetadata(target);
					}
					
					source.getConnection().setAutoCommit(false);
					target.getConnection().setAutoCommit(false);

					mover.exportDatabase();
					
					source.close();
					target.close();
					
					System.out.println("Data mover ENDED!" +	getCurrentTime());
				    long t2 = System.currentTimeMillis();
				    System.out.println(" Took " + (t2 - t1) / 1000 + " seconds to run the job!");
					
				} catch (Exception e) {
					e.printStackTrace();
					throw e;
				}

		    }

	        @Override
	        public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
	            
	            try {
					doJob();
				}
	            catch (Exception e) {
	                logger.error(e.getMessage(), e);
	            }

	        }
	        
	    }

}