package com.heroku.syncdbs;

import java.util.UUID;

public class TestQWorker {

	public TestQWorker() {
	}

	public static void main(String[] args) throws Exception {
		TestQWorker q = new TestQWorker();
		q.run();
	}

	public void run() throws Exception {
		SyncDB syncDB = new SyncDB();
		syncDB.connectBothDBsUsingJDBC();

		try {

			JobMessage jm = new JobMessage();
			jm.setTable("do_not_go_to_cyan");
			jm.setOffset(0);
			jm.setMaxid(150000);
			jm.setJobnum(1);
			jm.setJobid(UUID.randomUUID().toString());
			System.out.println("QWorker Job ID[" + jm.getJobid() + "]");

			syncDB.dropAndRecreateTableInTargetIfExists(jm.getTable(), 120324);

			JobLoggerHelper.logInitialTask(syncDB.getSource(), jm);

			syncDB.copyTableChunk(jm);
		} catch (Exception e) {
			throw e;
		} finally {
			syncDB.closeBothConnections();
		}
	}
}
