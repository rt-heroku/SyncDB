package com.heroku.syncdbs;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.heroku.syncdbs.enums.JobStatus;
import com.rabbitmq.client.QueueingConsumer;

public class LogWorker {
	private QueueManager logQ = new QueueManager();
	private SyncDB syncDB;
	private boolean shutdown = false;
	private boolean running = false;
	
	public LogWorker() {
	}

	public static void main(String[] args) throws Exception {
		final LogWorker q = new LogWorker();
		q.syncDB = new SyncDB();

		Runtime.getRuntime().addShutdownHook(new Thread() {
	        @Override
	            public void run() {
	        		try {
	        			System.out.println("Shuting down Log Worker!");
	        			q.shutdown = true;
	        			if (!q.running)
	        				q.syncDB.closeBothConnections();
					} catch (Exception e) {
						e.printStackTrace();
					}	
	            }   
	    }); 
		
		q.run();
	}

	public void run() throws Exception {
		syncDB.connectBothDBs();

		logQ.connect(Settings.getLogQueueName());
		
		try {
			while (true) {
				QueueingConsumer.Delivery delivery = logQ.nextDelivery();
				if (delivery != null) {
					running = true;
					long t1 = System.currentTimeMillis();
					JobMessage jm = logQ.parseJsonMessage(delivery.getBody());
					JobLoggerHelper.logTaskStatus(syncDB.getSource(), jm.getJobid(), jm.getTasknum(), jm.getTable().getFullName(), jm.getStatus());

					logQ.ack(delivery);
					if (jm.getStatus() == JobStatus.FINISHED)
						JobLoggerHelper.analyzeJobTask(syncDB.getSource(), jm);
					
					logEndMessage(t1, jm);
					
					running = false;
					
					if (shutdown)
						syncDB.closeBothConnections();
					
				}
			}
		} catch (Exception e) {
			syncDB.closeBothConnections();
			throw e;
		}
	}

	protected static String getCurrentTime() {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		return sdf.format(cal.getTime());
	}
	protected void logEndMessage(long t1, JobMessage jm) {
		String logmsg;
		long seconds = (System.currentTimeMillis() - t1) / 1000;
		logmsg = "LogWorker Finished " + getCurrentTime() + " Job ID [" + jm.getJobid() + "] " + jm.getTasknum()
				+ " of " + jm.getTotalTasks() + " in " + seconds + " seconds for table " + jm.getTable() + "";
		System.out.println(logmsg);
	}
}
