package com.heroku.syncdbs;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class ScheduleJob {

	final static ConnectionFactory factory = new ConnectionFactory();

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

			factory.setUri(System.getenv("CLOUDAMQP_URL"));
			scheduler.scheduleJob(jobDetail, trigger);

		} catch (SchedulerException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static class CopyDatabaseJob implements Job {

		@SuppressWarnings("unchecked")
		@Override
		public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
			try {
				logJob(jobExecutionContext);
				ConnectionFactory factory = new ConnectionFactory();
				factory.setUri(System.getenv("CLOUDAMQP_URL"));

				Connection connection = factory.newConnection();
				Channel channel = connection.createChannel();
				String queueName = "" + System.getenv("QUEUE_NAME");
				Map<String, Object> params = new HashMap<String, Object>();
				params.put("x-ha-policy", "all");
				channel.queueDeclare(queueName, true, false, false, params);

				Main main = new Main();
				main.connectBothDBs();
				Map<String, Integer> tables = main.getTablesAndCount();
				int chunk = getChunkSize(100000);

				for (String table : tables.keySet()) {
					int count = tables.get(table);
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

						channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN,
								obj.toJSONString().getBytes("UTF-8"));

						System.out.println("MANUALLY Publishing job number[" + job + "] for TABLE[" + table
								+ "] with total " + count + " rows - OFFSET: " + offset + " - LIMIT: " + chunk);

						offset = offset + chunk;
					}
				}

				main.closeBothConnections();
				connection.close();

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
