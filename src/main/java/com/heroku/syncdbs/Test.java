package com.heroku.syncdbs;

public class Test {

	public static void main(String[] args) {
		
		int count = 3945352;
		int chunk = 100000;
		int numberOfJobs = 0;
		int jobChunk = count;
		int offset = 0;
		while ( jobChunk > 0){
			
			jobChunk = jobChunk - chunk;
			numberOfJobs++;
			System.out.println("job number[" + numberOfJobs + "] - OFFSET: " + offset + " - LIMIT: " + chunk);
			offset = offset + chunk;
		}
		
	}

}
