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

		Main main = new Main();
		main.connectBothDBsUsingJDBC();
		String fromSchema = main.getFromSchema();
		String toSchema = main.getToSchema();

		try {

					String table = "do_not_go_to_cyan";
					Integer offset = 0;
					Integer limit = 150000;
					Integer job = 1;
					String jobid = UUID.randomUUID().toString();//"cef5ff23-c8f8-42d0-94ee-6609cdf0d69a";
					System.out.println("QWorker Job ID[" + jobid + "]");
					
					main.dropAndRecreateTableInTargetIfExists(table, 120324);
					
					JobLoggerHelper.logInitialTask(main.getSource(), table, jobid, job.toString());
					
					main.copyTableChunk(fromSchema, toSchema, table, offset, limit, job, jobid);

					main.closeBothConnections();

		} catch (Exception e) {
			main.closeBothConnections();
			throw e;
		}
	}
}
