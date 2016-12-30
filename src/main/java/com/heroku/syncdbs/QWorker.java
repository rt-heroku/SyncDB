package com.heroku.syncdbs;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.heroku.syncdbs.enums.JobStatus;
import com.rabbitmq.client.QueueingConsumer;

public class QWorker {
	public static final String LOG_QUEUE_NAME = "LOG_QUEUE_NAME";
	public static final String WORKER_QUEUE_NAME = "QUEUE_NAME";
	private QueueManager workerQ = new QueueManager();
	private QueueManager logQ = new QueueManager();
	
	public QWorker() throws Exception {
	}

	public static void main(String[] args) throws Exception {
		QWorker q = new QWorker();
		q.run();
	}

	public void run() throws Exception {
		SyncDB syncDB = new SyncDB();
		syncDB.connectBothDBs();
		
		workerQ.connect(System.getenv(WORKER_QUEUE_NAME));
		logQ.connect(System.getenv(LOG_QUEUE_NAME));
		
		try {
			while (true) {
				QueueingConsumer.Delivery delivery = workerQ.nextDelivery();
				if (delivery != null) {
					long t1 = System.currentTimeMillis();
					JobMessage jm = new JobMessage(delivery.getBody());

					jm.setStatus(JobStatus.PROCESSING);
					logQ.sendMessage(jm);
					
					logStartMessage(jm);

					syncDB.copyTableChunk(jm);

					workerQ.ack(delivery);

					logEndMessage(t1, jm);
					
					jm.setStatus(JobStatus.FINISHED);
//					logQ.sendMessage(jm);
				}
			}
		} catch (Exception e) {
			syncDB.closeBothConnections();
			throw e;
		}
	}


	protected void logStartMessage(JobMessage jm) {
		String current = getCurrentTime();
		String logmsg = "QWorker Starting [" + current + "] Job ID[" + jm.getJobid() + "] (" + jm.getJobnum() + " of "
				+ jm.getTotalJobs() + ") ---- Message Received: " + jm.toJson();
		System.out.println(logmsg);
	}

	protected void logEndMessage(long t1, JobMessage jm) {
		String logmsg;
		long seconds = (System.currentTimeMillis() - t1) / 1000;
		logmsg = "QWorker ENDED     [" + getCurrentTime() + "] Job ID [" + jm.getJobid() + "] (" + jm.getJobnum()
				+ " of " + jm.getTotalJobs() + ") in [" + seconds + "] seconds for table [" + jm.getTable() + "] !";
		System.out.println(logmsg);
	}

	protected static String getCurrentTime() {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		return sdf.format(cal.getTime());
	}

}
