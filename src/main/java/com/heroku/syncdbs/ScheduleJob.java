package com.heroku.syncdbs;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.text.SimpleDateFormat;
import java.util.Map;

import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

public class ScheduleJob {

	public static void main(String[] args) {
		try {
			Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
			
			scheduler.start();

			JobDetail jobDetail = newJob(CopyDatabaseJob.class).build();

			String schedule = "" + System.getenv("SCHEDULE_CRON");
			System.out.println("schedule " + schedule);
			
			CronTrigger trigger = newTrigger().withIdentity("trigger1", "group1").startNow()
					.withSchedule(cronSchedule(schedule)).build();

			System.out.println("Schedule to run: " + schedule);
			System.out.println(trigger.getExpressionSummary());

			scheduler.scheduleJob(jobDetail, trigger);

		} catch (SchedulerException se) {
			se.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
		}

	}

	public static class CopyDatabaseJob implements Job {

		@Override
		public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
			try {
				logJob(jobExecutionContext);
				Map<String, Integer> tables = Main.getTablesAndCount();
				
				for (String table : tables.keySet()){
					int count = tables.get(table);
					System.out.println("Creating job for TABLE[" + table + "] for " + count + " rows...");
				}
				
			} catch (Exception e) {
				System.err.println(e.getMessage());
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
