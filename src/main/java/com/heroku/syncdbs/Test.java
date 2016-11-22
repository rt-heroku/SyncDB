package com.heroku.syncdbs;

public class Test {

	public static void main(String[] args) {
		
		int count = 3945352;
		int chunk = 100000;
		int numberOfJobs = 0;
		int jobChunk = count;
		int offset = chunk;
		while ( jobChunk > 0){
			
			int jc = jobChunk;
			if ((jobChunk - chunk) > 0) 
				jc = chunk;
			jobChunk = jobChunk - chunk;
			numberOfJobs++;
			System.out.println("job number[" + numberOfJobs + "] - OFFSET: " + offset + " - LIMIT: " + jc);
			offset = offset + chunk;
		}
		
	}

}
