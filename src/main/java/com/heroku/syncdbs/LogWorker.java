package com.heroku.syncdbs;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.heroku.syncdbs.enums.JobStatus;
import com.rabbitmq.client.QueueingConsumer;

public class LogWorker {
	public static final String LOG_QUEUE_NAME = "LOG_QUEUE_NAME";
	private QueueManager logQ = new QueueManager();

	public LogWorker() {
	}

	public static void main(String[] args) throws Exception {
		LogWorker q = new LogWorker();
		q.run();
	}

	public void run() throws Exception {
		SyncDB syncDB = new SyncDB();
		syncDB.connectBothDBs();
		
		logQ.connect(System.getenv(LOG_QUEUE_NAME));
		
		try {
			while (true) {
				QueueingConsumer.Delivery delivery = logQ.nextDelivery();
				if (delivery != null) {
					long t1 = System.currentTimeMillis();
					JobMessage jm = new JobMessage(delivery.getBody());

					JobLoggerHelper.logTaskStatus(syncDB.getSource(), jm.getJobid(), jm.getTasknum(), jm.getTable(), jm.getStatus());

					logQ.ack(delivery);
					
					if (jm.getStatus() == JobStatus.FINISHED)
						JobLoggerHelper.analyzeJobTask(syncDB.getSource(), jm);
					
					logEndMessage(t1, jm);
					
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
		logmsg = "LogWorker ENDED     [" + getCurrentTime() + "] Job ID [" + jm.getJobid() + "] (" + jm.getJobnum()
				+ " of " + jm.getTotalJobs() + ") in [" + seconds + "] seconds for table [" + jm.getTable() + "] !";
		System.out.println(logmsg);
	}
}
