package com.heroku.syncdbs;

import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;

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
			TableInfo ti = new TableInfo("transfer","\"Seal_DGR_AdminForm\"","",0,71);
			jm.setTable(ti);
			jm.setOffset(0);
			jm.setMaxid(64333);
			jm.setJobnum(1);
			jm.setJobid(UUID.randomUUID().toString());
			jm.setTasknum(1);
			jm.setChunk(20000);
			System.out.println("QWorker Job ID[" + jm.getJobid() + "]");

			//JSONObject jo = new JSONObject();
			
			System.out.println(jm.toJson());
			//{"jobid":"8de7aca6-5dcd-42d1-8a16-d5617a4f5d43","table":{"schema":"transfer","name":"\"Seal_DGR_AdminForm\"","type":"","count":0,"maxid":71,"fullName":"transfer.null","analyze":false,"refresh":false,"transfer":true},"maxid":64333,"offset":0,"chunk":20000,"jobnum":1,"totalTasks":null,"last":null,"status":"CREATED","tasknum":1}

			String json = jm.toJson();
			
			try{
				JobMessage newJm = null;
				ObjectMapper mapper = new ObjectMapper();
				newJm = mapper.readValue(json.getBytes(), JobMessage.class);
				System.out.println(newJm.getJobid());
			}catch (Exception e) {
			}
			
			syncDB.dropAndRecreateTableInTargetIfExists(jm.getTable(), 120324);

//			JobLoggerHelper.logInitialTask(syncDB.getSource(), jm);

			syncDB.copyTableChunk(jm);
		} catch (Exception e) {
			throw e;
		} finally {
			syncDB.closeBothConnections();
		}
	}
}
