package com.heroku.syncdbs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.heroku.syncdbs.enums.JobStatus;

public class JobMessage {

	private String jobid;
	private TableInfo table;
	private Integer maxid;
	private Integer offset;
	private Integer chunk;
	private Integer jobnum;
	private Integer totalTasks;
	private Boolean last;
	private JobStatus status;
	private Integer tasknum;
	
	public JobMessage(){
		super();
	}
	
	public JobMessage(String jobid, TableInfo table, Integer maxid, Integer offset, Integer chunk, Integer jobnum,
			Integer totalJobs, Boolean last, Integer tasknum) {
		super();
		this.jobid = jobid;
		this.table = table;
		this.maxid = maxid;
		this.offset = offset;
		this.chunk = chunk;
		this.jobnum = jobnum;
		this.totalTasks = totalJobs;
		this.last = last;
		this.status = JobStatus.CREATED;
		this.tasknum = tasknum;
	}

	public String toJson() {
		try{
			ObjectMapper mapper = new ObjectMapper();
			return mapper.writeValueAsString(this);
		}catch (Exception e) {
			return "";
		}
	}

	public String getJobid() {
		return jobid;
	}
	public void setJobid(String jobid) {
		this.jobid = jobid;
	}
	public TableInfo getTable() {
		return table;
	}
	public void setTable(TableInfo table) {
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
	public Integer getTotalTasks() {
		return totalTasks;
	}
	public void setTotalTasks(Integer totalTasks) {
		this.totalTasks = totalTasks;
	}
	public Boolean getLast() {
		return last;
	}
	public void setLast(Boolean last) {
		this.last = last;
	}

	public JobStatus getStatus() {
		if (status == null)
			return JobStatus.CREATED;
		
		return status;
	}

	public void setStatus(JobStatus status) {
		this.status = status;
	}

	public Integer getTasknum() {
		return tasknum;
	}

	public void setTasknum(Integer tasknum) {
		this.tasknum = tasknum;
	}
	
}
