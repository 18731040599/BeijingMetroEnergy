package com.thtf.entity;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateEntity {
	private String currentTime;
	private String startTime;
	private String laseday;

	public DateEntity() {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MINUTE, -calendar.get(Calendar.MINUTE) % 20);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm00");
		currentTime = simpleDateFormat.format(calendar.getTime());
		calendar.add(Calendar.MINUTE, -20);
		startTime = simpleDateFormat.format(calendar.getTime());
		calendar.add(Calendar.MINUTE, -(20 * 71));
		laseday = simpleDateFormat.format(calendar.getTime());
	}

	public String getCurrentTime() {
		return currentTime;
	}

	public String getStartTime() {
		return startTime;
	}

	public String getYesterday() {
		return laseday;
	}

}
