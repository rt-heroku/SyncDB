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

import com.heroku.syncdbs.enums.JobType;

public class ScheduleJob {

	private static final String JOB_USER = "SCHEDULER";

	public static void main(String[] args) {
		try {
			Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

			scheduler.start();

			JobDetail jobDetail = newJob(CopyDatabaseJob.class).build();

			String schedule = "" + System.getenv("SCHEDULE_CRON");
			String description = "CopyDatabase Job scheduled to run [" + schedule + "]";

			CronTrigger trigger = newTrigger().withIdentity("trigger1", "group1").startNow()
					.withSchedule(cronSchedule(schedule)).withDescription(description).build();

			System.out.println("Schedule to run: " + schedule);
			System.out.println(trigger.getExpressionSummary());

			scheduler.scheduleJob(jobDetail, trigger);

		} catch (SchedulerException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static class CopyDatabaseJob implements Job {

		@Override
		public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
			try {
				RunJob rj = new RunJob();
				logJob(jobExecutionContext);
				rj.runJob(JobType.SCHEDULED, JOB_USER);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		private void logJob(JobExecutionContext jobExecutionContext) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			System.out.println("Running Job: " + jobExecutionContext.getJobDetail().getDescription());
			System.out.println("Next Job will run on: " + sdf.format(jobExecutionContext.getNextFireTime()));
		}

	}

}
