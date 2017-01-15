package com.heroku.syncdbs;

public class Settings {
	private static final String SCHEDULE_CRON = "SCHEDULE_CRON";
	private static final String RABBITMQ_BIGWIG_URL = "RABBITMQ_BIGWIG_URL";
	private static final String DEBUG2 = "DEBUG";
	private static final String CHUNK_SIZE = "CHUNK_SIZE";
	private static final String TRANSFER_TO_SCHEMA = "TRANSFER_TO_SCHEMA";
	private static final String TRANSFER_FROM_SCHEMA = "TRANSFER_FROM_SCHEMA";
	private static final String LOG_QUEUE_NAME = "LOG_QUEUE_NAME";
	private static final String WORKER_QUEUE_NAME = "QUEUE_NAME";
	private static final String MV_SCHEMA_VARNAME = "MV_SCHEMA";
	private static final String VIEWS_TO_REFRESH_VARNAME = "VIEWS_TO_REFRESH";
	private static final String SOURCE_VAR = "SOURCE_VAR";
	private static final String TARGET_VAR = "TARGET_VAR";
	private static final String USE_VIEW_INVENTORY = "USE_VIEW_INVENTORY";
	private static final String ANALYZE_BEFORE_PROCESS = "ANALYZE_BEFORE_PROCESS";
	private static final String REFRESH_VIEWS = "REFRESH_VIEWS";
	
	private static String logQueueName;
	private static String workerQueueName;
	private static String mvSchemaVarName;
	private static String viewsToRefreshVarName;
	private static String transferFromSchema;
	private static String transferToSchema;
	private static String targetVar;
	private static String sourceVar;
	private static String chunkSize;
	private static String debug;
	private static String queueUrl;
	private static String schedulerCron;
	private static String useViewInventory;
	private static String analyzeBeforeProcess;
	private static String refreshViews;
	
	static {
		logQueueName = System.getenv(LOG_QUEUE_NAME);
		workerQueueName = System.getenv(WORKER_QUEUE_NAME);
		mvSchemaVarName = System.getenv(MV_SCHEMA_VARNAME);
		viewsToRefreshVarName = System.getenv(VIEWS_TO_REFRESH_VARNAME);
		transferFromSchema = System.getenv(TRANSFER_FROM_SCHEMA);
		transferToSchema = System.getenv(TRANSFER_TO_SCHEMA);
		targetVar = System.getenv(TARGET_VAR);
		sourceVar = System.getenv(SOURCE_VAR);
		chunkSize = System.getenv(CHUNK_SIZE);
		debug = System.getenv(DEBUG2);
		queueUrl = System.getenv(RABBITMQ_BIGWIG_URL);
		schedulerCron = System.getenv(SCHEDULE_CRON);
		useViewInventory = System.getenv(USE_VIEW_INVENTORY);
		analyzeBeforeProcess = System.getenv(ANALYZE_BEFORE_PROCESS);
		refreshViews = System.getenv(REFRESH_VIEWS);
	}

	public static String getEnvVar(String var, String defaultValue) {
		String s = System.getenv(var);
		if (s == null)
			return defaultValue;
		else
			return s;

	}

	public static String getDefaultVal(String s, String defaultValue) {
		if (s == null)
			return defaultValue;
		else
			return s;
	}

	public static String getLogQueueName() {
		return logQueueName;
	}

	public static String getWorkerQueueName() {
		return workerQueueName;
	}

	public static String getMvSchema() {
		if (mvSchemaVarName == null)
			return mvSchemaVarName;
		else
			return mvSchemaVarName;
	}

	public static String getViewsToRefresh() {
		if (viewsToRefreshVarName == null)
			return "";
		else
			return viewsToRefreshVarName;
	}

	public static String getTransferFromSchema() {
		return transferFromSchema;
	}

	public static String getTransferToSchema() {
		return transferToSchema;
	}

	public static String getTargetVar() {
		return targetVar;
	}

	public static String getSourceVar() {
		return sourceVar;
	}

	public static String getChunkSize() {
		return chunkSize;
	}

	public static boolean isDebug() {
		if (debug == null)
			return false;
		else 
			return debug.equals("true");
	}

	public static String getQueueUrl() {
		return queueUrl;
	}

	public static String getSchedulerCron() {
		return schedulerCron;
	}

	public static boolean useViewInventory() {
		
		if (useViewInventory == null)
			return false;
		else
			return useViewInventory.equals("true");
	}

	public static boolean analyzeBeforeProcess() {
		if (analyzeBeforeProcess == null)
			return false;
		else
			return analyzeBeforeProcess.equals("true");
	}

	public static boolean refreshViews() {
		if (refreshViews == null)
			return false;
		else
			return refreshViews.equals("true");
	}

}
