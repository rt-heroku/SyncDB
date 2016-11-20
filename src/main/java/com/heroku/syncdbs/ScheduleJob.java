package com.heroku.syncdbs;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.text.SimpleDateFormat;

import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduleJob {
	final static Logger logger = LoggerFactory.getLogger(ScheduleJob.class);

	public static void main(String[] args) {
		try {
			Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

			scheduler.start();

			JobDetail jobDetail = newJob(CopyDatabaseJob.class).build();

			String schedule = "" + System.getenv("SCHEDULE_CRON");
System.out.println("schedule " + schedule);
			CronTrigger trigger = newTrigger().withIdentity("trigger1", "group1").startNow()
					.withSchedule(cronSchedule(schedule)).build();

			logger.info("Schedule to run: " + schedule + "\n" + trigger.getExpressionSummary());

			scheduler.scheduleJob(jobDetail, trigger);

		} catch (SchedulerException se) {
			se.printStackTrace();
		}

	}

	public static class CopyDatabaseJob implements Job {

		@Override
		public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				System.out.println("Running Job: " + jobExecutionContext.getJobDetail().getDescription());
				System.out.println("Next Job will run on: " + sdf.format(jobExecutionContext.getNextFireTime()));
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}

		}

	}

}
