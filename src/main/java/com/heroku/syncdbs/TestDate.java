package com.heroku.syncdbs;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestDate {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(getTimestampNow());
		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
		System.out.println(timeStamp);
	}
	private static Timestamp getTimestampNow() {
		Date d = new Date();
		return new Timestamp(d.getTime());
	}


}
