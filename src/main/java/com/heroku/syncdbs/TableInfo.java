package com.heroku.syncdbs;

public class TableInfo {

	private String schema;
	private String name;
	private String type;
	private int count;
	private int maxid;
	private String fullName;
	private boolean analyze;
	private boolean refresh;
	private boolean transfer;
	
	public TableInfo(String schema, String name, String type, int count, int maxid, boolean analyze, boolean refresh) {
		super();
		this.schema = schema;
		this.name = name;
		this.type = type;
		this.count = count;
		this.maxid = maxid;
		this.analyze = analyze;
		this.refresh = refresh;
		this.transfer = true;
	}

	public TableInfo() {
		super();
	}

	public TableInfo(String schema, String name) {
		super();
		this.schema = schema;
		this.name = name;
		this.type = "";
		this.count = 0;
		this.maxid = 0;
		this.analyze = false;
		this.refresh = false;
		this.transfer = true;
	}

	public TableInfo(String schema, String name, String type) {
		super();
		this.schema = schema;
		this.name = name;
		this.type = type;
		this.count = 0;
		this.maxid = 0;
		this.analyze = false;
		this.refresh = false;
		this.transfer = true;
	}

	public TableInfo(String schema, String name, String type, int count, int maxid) {
		super();
		this.schema = schema;
		this.name = name;
		this.type = type;
		this.count = count;
		this.maxid = maxid;
		this.analyze = false;
		this.refresh = false;
		this.transfer = true;
	}

	public String getSchema() {
		return schema;
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	public int getCount() {
		return count;
	}

	public int getMaxid() {
		return maxid;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public void setMaxid(int maxid) {
		this.maxid = maxid;
	}
	
	public String getFullName() {
		return schema + "." + fullName;
	}

	public void setFullName(String fullName){
		this.fullName = fullName;
	}

	public boolean isAnalyze() {
		return analyze;
	}

	public boolean isRefresh() {
		return refresh;
	}

	public void setAnalyze(boolean analyze) {
		this.analyze = analyze;
	}

	public void setRefresh(boolean refresh) {
		this.refresh = refresh;
	}

	public boolean isTransfer() {
		return transfer;
	}

	public void setTransfer(boolean transfer) {
		this.transfer = transfer;
	}

}
