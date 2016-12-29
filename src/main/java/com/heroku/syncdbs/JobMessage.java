package com.heroku.syncdbs;

import java.io.UnsupportedEncodingException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JobMessage {

	private String jobid;
	private String table;
	private Integer maxid;
	private Integer offset;
	private Integer chunk;
	private Integer jobnum;
	private Integer totalJobs;
	private Boolean last;
	
	private JSONParser parser = null;
	private JSONObject jobj = null;
	
	public JobMessage(){
		super();
	}
	
	public JobMessage(byte[] b) throws UnsupportedEncodingException, ParseException{
		String msg = new String(b, "UTF-8");
		setJsonAndParseMessage(msg);
	}
	
	public JobMessage(String json) throws ParseException{
		setJsonAndParseMessage(json);
	}

	public JobMessage(JSONObject jobj){
		this.jobj = jobj;
		setJobMessage(jobj);
	}
	
	public JobMessage(String jobid, String table, Integer maxid, Integer offset, Integer chunk, Integer jobnum,
			Integer totalJobs, Boolean last) {
		super();
		this.jobid = jobid;
		this.table = table;
		this.maxid = maxid;
		this.offset = offset;
		this.chunk = chunk;
		this.jobnum = jobnum;
		this.totalJobs = totalJobs;
		this.last = last;
	}

	private void setJsonAndParseMessage(String json) throws ParseException {
		this.parser = new JSONParser();
		this.jobj = (JSONObject) parser.parse(json);
		setJobMessage(jobj);
	}

	private void setJobMessage(JSONObject jobj){

		this.jobid = (String) jobj.get("jobid");
		this.table = (String) jobj.get("table");
		this.offset = new Integer(jobj.get("offset").toString());
		this.chunk = new Integer(jobj.get("chunk").toString());
		this.jobnum = new Integer(jobj.get("jobnum").toString());
		this.totalJobs = new Integer(jobj.get("totaljobs").toString());
		this.maxid = new Integer(jobj.get("maxid").toString());
		
	}
	
	@SuppressWarnings("unchecked")
	public JSONObject getNewJsonObject(){
		JSONObject obj = new JSONObject();
		obj.put("jobid", getJobid());
		obj.put("table", getTable());
		obj.put("maxid", getMaxid());
		obj.put("offset", getOffset());
		obj.put("chunk", getChunk());
		obj.put("jobnum", getJobnum());
		obj.put("last", getLast());
		obj.put("totaljobs", getTotalJobs());
		return obj;
	}
	
	@SuppressWarnings("unchecked")
	public JSONObject getJsonObject(){
		jobj.put("jobid", getJobid());
		jobj.put("table", getTable());
		jobj.put("maxid", getMaxid());
		jobj.put("offset", getOffset());
		jobj.put("chunk", getChunk());
		jobj.put("jobnum", getJobnum());
		jobj.put("last", getLast());
		jobj.put("totaljobs", getTotalJobs());
		return jobj;
	}

	public String toJson() {
		return getJsonObject().toJSONString();
	}

	public String getJobid() {
		return jobid;
	}
	public void setJobid(String jobid) {
		this.jobid = jobid;
	}
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public Integer getMaxid() {
		return maxid;
	}
	public void setMaxid(Integer maxid) {
		this.maxid = maxid;
	}
	public Integer getOffset() {
		return offset;
	}
	public void setOffset(Integer offset) {
		this.offset = offset;
	}
	public Integer getChunk() {
		return chunk;
	}
	public void setChunk(Integer chunk) {
		this.chunk = chunk;
	}
	public Integer getJobnum() {
		return jobnum;
	}
	public void setJobnum(Integer jobnum) {
		this.jobnum = jobnum;
	}
	public Integer getTotalJobs() {
		return totalJobs;
	}
	public void setTotalJobs(Integer totalJobs) {
		this.totalJobs = totalJobs;
	}
	public Boolean getLast() {
		return last;
	}
	public void setLast(Boolean last) {
		this.last = last;
	}
	
}
