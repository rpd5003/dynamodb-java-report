package com.channelape.completedtasks.reports.data.models;

import java.util.Date;

public class ReportModel implements Comparable<ReportModel>{

	/*
	 ------------------------------------------------------------------------------------
	 #"Business ID" - CompletedTasks:businessId
	 #"Business Name" - CompletedTasks:businessId join Businesses:id for Businesses:name
	 #"Target ID" - CompletedTasks:targetId 
	 #"Target Type" - CompletedTasks:targetType
	 #"Target Name" - CompletedTasks:targetId to join to Channels or Suppliers based on targetType
	 #"Integration Name" - CompletedTasks:targetId join Channels:id for Channels:name
	 #"SKU" - CompletedTasks:sku
	 #"UPC" - CompletedTasks:upc
	 #"Operation" - CompletedTasks:operation
	 #"Description" - CompletedTasks:description
	 #"Completion Time" - CompletedTasks:completionTime
	 ------------------------------------------------------------------------------------
	 */


	private String businessId;
	private String businessName;
	private String targetId;
	private String targetType;
	private String targetName;
	private String integrationName;
	private String sku;
	private String upc;
	private String operation;
	private String description;
	private Date completionTime;
	
	public String getBusinessId() {
		return businessId;
	}
	public void setBusinessId(String businessId) {
		this.businessId = businessId;
	}
	public String getBusinessName() {
		return businessName;
	}
	public void setBusinessName(String businessName) {
		this.businessName = businessName;
	}
	public String getTargetId() {
		return targetId;
	}
	public void setTargetId(String targetId) {
		this.targetId = targetId;
	}
	public String getTargetType() {
		return targetType;
	}
	public void setTargetType(String targetType) {
		this.targetType = targetType;
	}
	public String getTargetName() {
		return targetName;
	}
	public void setTargetName(String targetName) {
		this.targetName = targetName;
	}
	public String getIntegrationName() {
		return integrationName;
	}
	public void setIntegrationName(String integrationName) {
		this.integrationName = integrationName;
	}
	public String getSku() {
		return sku;
	}
	public void setSku(String sku) {
		this.sku = sku;
	}
	public String getUpc() {
		return upc;
	}
	public void setUpc(String upc) {
		this.upc = upc;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public Date getCompletionTime() {
		return completionTime;
	}
	public void setCompletionTime(Date completionTime) {
		this.completionTime = completionTime;
	}
	@Override
    public int compareTo(ReportModel o) {
        return this.getCompletionTime().compareTo(o.getCompletionTime());
    }
}
