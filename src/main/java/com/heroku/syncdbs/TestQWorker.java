package com.heroku.syncdbs;

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

					String table = "filtered_accounts_raj";
					Integer offset = 0;
					Integer limit = 20000;
					Integer job = 1;

					System.out.println("QWorker Job ID[" + job + "]");

					main.copyTableChunk(fromSchema, toSchema, table, offset, limit, job);

		} catch (Exception e) {
			main.closeBothConnections();
			throw e;
		}
	}
}
