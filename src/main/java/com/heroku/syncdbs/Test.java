package com.heroku.syncdbs;

import java.util.Map;

import org.json.simple.JSONObject;

public class Test {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
	try{	
		Main main = new Main();
		main.connectBothDBs();
		Map<String, Integer> tables = main.getTablesAndCount();
		int chunk = getChunkSize(100000);

		for (String table : tables.keySet()) {
			int count = tables.get(table).intValue();
			JSONObject obj = new JSONObject();

			int job = 0;
			int jobChunk = count;
			int offset = 0;

			main.recreateTable(table);

			while (jobChunk > 0) {

				jobChunk = jobChunk - chunk;
				job++;

				obj.put("table", table);
				obj.put("count", count);
				obj.put("offset", offset);
				obj.put("limit", chunk);
				obj.put("job", job);

				System.out.println("MANUALLY Publishing job number[" + job + "] for TABLE[" + table
						+ "] with total " + count + " rows - OFFSET: " + offset + " - LIMIT: " + chunk);

				offset = offset + chunk;
			}

		}

		main.closeBothConnections();
	} catch (Exception e) {
		System.err.println(e.getMessage());
		e.printStackTrace();
	}
		
	}
	private static int getChunkSize(int i) {
		int r = 0;
		try {
			String cs = System.getenv("CHUNK_SIZE");
			if (cs == null || cs.equals(""))
				return i;
			r = new Integer(cs).intValue();
		} catch (Exception e) {
			return i;
		}

		return r;
	}

}
