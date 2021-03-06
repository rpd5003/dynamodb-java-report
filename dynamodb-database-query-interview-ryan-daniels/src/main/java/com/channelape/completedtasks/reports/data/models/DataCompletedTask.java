package com.channelape.completedtasks.reports.data.models;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAutoGeneratedKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = DataCompletedTask.TABLE_NAME)
public class DataCompletedTask {

	public static final String TABLE_NAME = "CompletedTasks";
	public static final String ID = "id";
	public static final String BUSINESS_ID = "businessId";
	public static final String COMPLETION_TIME = "completionTime";
	public static final String TARGET_ID = "targetId";
	public static final String TARGET_TYPE = "targetType";

	private static final String SKU = "sku";
	private static final String UPC = "upc";
	private static final String OPERATION = "operation";
	private static final String DESCRIPTION = "description";

	private String id;
	private String businessId;
	private String sku;
	private String upc;
	private String targetId;
	private String targetType;
	private String operation;
	private String description;
	private long completionTimeInMilliseconds;

	@DynamoDBHashKey(attributeName = ID)
	@DynamoDBAutoGeneratedKey
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@DynamoDBAttribute(attributeName = BUSINESS_ID)
	public String getBusinessId() {
		return businessId;
	}

	public void setBusinessId(String businessId) {
		this.businessId = businessId;
	}

	@DynamoDBAttribute(attributeName = SKU)
	public String getSku() {
		return sku;
	}

	public void setSku(String sku) {
		this.sku = sku;
	}

	@DynamoDBAttribute(attributeName = UPC)
	public String getUpc() {
		return upc;
	}

	public void setUpc(String upc) {
		this.upc = upc;
	}

	@DynamoDBAttribute(attributeName = TARGET_ID)
	public String getTargetId() {
		return targetId;
	}

	public void setTargetId(String targetId) {
		this.targetId = targetId;
	}

	@DynamoDBAttribute(attributeName = TARGET_TYPE)
	public String getTargetType() {
		return targetType;
	}

	public void setTargetType(String targetType) {
		this.targetType = targetType;
	}

	@DynamoDBAttribute(attributeName = OPERATION)
	public String getOperation() {
		return operation;
	}

	public void setOperation(String operation) {
		this.operation = operation;
	}

	@DynamoDBAttribute(attributeName = DESCRIPTION)
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@DynamoDBAttribute(attributeName = COMPLETION_TIME)
	public long getCompletionTimeInMilliseconds() {
		return completionTimeInMilliseconds;
	}

	public void setCompletionTimeInMilliseconds(long completionTimeInMilliseconds) {
		this.completionTimeInMilliseconds = completionTimeInMilliseconds;
	}

}
