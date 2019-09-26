package com.channelape.completedtasks.reports.services;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.channelape.completedtasks.reports.data.models.DataBusiness;
import com.channelape.completedtasks.reports.data.models.DataChannel;
import com.channelape.completedtasks.reports.data.models.DataCompletedTask;
import com.channelape.completedtasks.reports.data.models.DataIntegration;
import com.channelape.completedtasks.reports.data.models.DataSupplier;
import com.channelape.completedtasks.reports.data.models.ReportModel;
import com.channelape.completedtasks.reports.services.models.Target;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

@Service
public class ReportGenerationService {

	static final List<String> HEADERS = Arrays.asList("Business ID", "Business Name", "Target ID", "Target Type",
			"Target Name", "Integration Name", "SKU", "UPC", "Operation", "Description", "Completion Time");

	private static final String FILE_PREFIX = "completed_tasks_report_";
	private static final String FILE_SUFFIX = ".csv";

	@Autowired
	private DynamoDBMapper dynamoDBMapper;

	public File generate(final DateTime earliestTaskCompletionTime, final DateTime latestTaskCompletionTime,
			final Target target) throws IOException {
		final File file = buildTemporaryFile();
		CSVPrinter csvPrinter = null;
		// TODO : Implement me
		try {
			/*
			 * Report Headers and their mappings
			 * -----------------------------------------------------------------------------
			 * #"Business ID" - CompletedTasks:businessId 
			 * #"Business Name" - CompletedTasks:businessId join Businesses:id for Businesses:name 
			 * #"Target ID" - CompletedTasks:targetId 
			 * #"Target Type" - CompletedTasks:targetType
			 * #"Target Name" - CompletedTasks:targetId to join to Channels or Suppliers based on targetType 
			 * #"Integration Name" - CompletedTasks:targetId join Channels:id for Channels:name 
			 * #"SKU" - CompletedTasks:sku 
			 * #"UPC" - CompletedTasks:upc 
			 * #"Operation" - CompletedTasks:operation 
			 * #"Description" - CompletedTasks:description 
			 * #"Completion Time" - CompletedTasks:completionTime
			 * -----------------------------------------------------------------------------
			 * -------
			 */

			// Create ExpectedAttributeValue map for Scan filter expression for
			// CompletedTasks
			// date range needs to be converted to millis to query CompletedTasks
			Map<String, AttributeValue> dataCompletedTaskEav = new HashMap<String, AttributeValue>();
			Map<String, String> attributeNames = new HashMap<String, String>();
			if(target.getType() == (Target.Type.BUSINESS)) {
				attributeNames.put("#targetType", "businessId");
			} else if (target.getType() == (Target.Type.CHANNEL) || target.getType() == (Target.Type.SUPPLIER)) {
				attributeNames.put("#targetType", "targetId");
			}
			dataCompletedTaskEav.put(":val1", new AttributeValue().withS(target.getId()));
			dataCompletedTaskEav.put(":val2",
					new AttributeValue().withN(String.valueOf(earliestTaskCompletionTime.getMillis())));
			dataCompletedTaskEav.put(":val3",
					new AttributeValue().withN(String.valueOf(latestTaskCompletionTime.getMillis())));

			// Scan CompletedTasks Table on targetId and date range and put results into
			// List
			DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
					.withFilterExpression("#targetType = :val1 and completionTime >= :val2 and completionTime <= :val3")
					.withExpressionAttributeNames(attributeNames)
					.withExpressionAttributeValues(dataCompletedTaskEav);
			List<DataCompletedTask> dct = dynamoDBMapper.scan(DataCompletedTask.class, scanExpression);

			System.out.println("DATA COMPLETED TASKS COUNT: " + dct.size());

			// Iterate over List<DataCompletedTask> populating report columns
			// using businessId to query Businesses
			// using targetId to query Channels or Suppliers based on targetType
			// using whatever to get integration name

			ArrayList<ReportModel> reportList = new ArrayList<ReportModel>();
			
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
			df.setTimeZone(TimeZone.getTimeZone("GMT")); //use time zone from business table?
			
			// Now create the CSV
			BufferedWriter writer = Files.newBufferedWriter(Paths.get(file.getPath()));
			csvPrinter = new CSVPrinter(writer,
					CSVFormat.RFC4180.withHeader("Business ID", "Business Name", "Target ID", "Target Type",
							"Target Name", "Integration Name", "SKU", "UPC", "Operation", "Description",
							"Completion Time"));

			Iterator<DataCompletedTask> iterator = dct.iterator();
			while (iterator.hasNext()) {
				DataCompletedTask dctCurrent = (DataCompletedTask) iterator.next();

				Map<String, AttributeValue> dataBusinessEav = new HashMap<String, AttributeValue>();
				dataBusinessEav.put(":val1", new AttributeValue().withS(dctCurrent.getBusinessId()));
				DynamoDBQueryExpression<DataBusiness> queryExpression = new DynamoDBQueryExpression<DataBusiness>()
						.withKeyConditionExpression("id = :val1").withExpressionAttributeValues(dataBusinessEav);
				List<DataBusiness> dB = dynamoDBMapper.query(DataBusiness.class, queryExpression);

				String targetName = null;
				String integrationId = null;
				String integrationName = null;
				
				if(dctCurrent.getTargetType().equalsIgnoreCase("supplier".trim())) {
					Map<String, AttributeValue> dataSupplierEav = new HashMap<String, AttributeValue>();
					dataSupplierEav.put(":val1", new AttributeValue().withS(dctCurrent.getTargetId()));
					DynamoDBQueryExpression<DataSupplier> queryExpression2 = new DynamoDBQueryExpression<DataSupplier>()
							.withKeyConditionExpression("id = :val1").withExpressionAttributeValues(dataSupplierEav);
					
					List<DataSupplier> dB2 = dynamoDBMapper.query(DataSupplier.class, queryExpression2);
					targetName = dB2.get(0).getName();
					integrationId = dB2.get(0).getIntegrationId();
					
					Map<String, AttributeValue> dataIntegrationEav = new HashMap<String, AttributeValue>();
					dataIntegrationEav.put(":val1", new AttributeValue().withS(integrationId));
					DynamoDBQueryExpression<DataIntegration> queryExpression3 = new DynamoDBQueryExpression<DataIntegration>()
							.withKeyConditionExpression("id = :val1").withExpressionAttributeValues(dataIntegrationEav);
					List<DataIntegration> dB3 = dynamoDBMapper.query(DataIntegration.class, queryExpression3);
					integrationName = dB3.get(0).getName();
					
				} else if (dctCurrent.getTargetType().equalsIgnoreCase("channel".trim())) {
					Map<String, AttributeValue> dataChannelEav = new HashMap<String, AttributeValue>();
					dataChannelEav.put(":val1", new AttributeValue().withS(dctCurrent.getTargetId()));
					DynamoDBQueryExpression<DataChannel> queryExpression2 = new DynamoDBQueryExpression<DataChannel>()
							.withKeyConditionExpression("id = :val1").withExpressionAttributeValues(dataChannelEav);
					
					List<DataChannel> dB2 = dynamoDBMapper.query(DataChannel.class, queryExpression2);
					targetName = dB2.get(0).getName();
					integrationId = dB2.get(0).getIntegrationId();
					
					Map<String, AttributeValue> dataIntegrationEav = new HashMap<String, AttributeValue>();
					dataIntegrationEav.put(":val1", new AttributeValue().withS(integrationId));
					DynamoDBQueryExpression<DataIntegration> queryExpression3 = new DynamoDBQueryExpression<DataIntegration>()
							.withKeyConditionExpression("id = :val1").withExpressionAttributeValues(dataIntegrationEav);
					List<DataIntegration> dB3 = dynamoDBMapper.query(DataIntegration.class, queryExpression3);
					integrationName = dB3.get(0).getName();
				}
				
				Date currentDate = new Date(dctCurrent.getCompletionTimeInMilliseconds());
								
				ReportModel rm = new ReportModel();
				rm.setBusinessId(dctCurrent.getBusinessId());
				rm.setBusinessName(dB.get(0).getName());
				rm.setTargetId(dctCurrent.getTargetId());
				rm.setTargetType(dctCurrent.getTargetType());
				rm.setTargetName(targetName);
				rm.setIntegrationName(integrationName);
				rm.setSku(dctCurrent.getSku());
				rm.setUpc(dctCurrent.getUpc());
				rm.setOperation(dctCurrent.getOperation());
				rm.setDescription(dctCurrent.getDescription());
				rm.setCompletionTime(currentDate);
				
				reportList.add(rm);
			}

			Collections.sort(reportList);
			Iterator<ReportModel> sortedIterator = reportList.iterator();
			while (sortedIterator.hasNext()) {
				ReportModel rmCurrent = (ReportModel) sortedIterator.next();
				csvPrinter.printRecord(rmCurrent.getBusinessId(), rmCurrent.getBusinessName(), rmCurrent.getTargetId(), rmCurrent.getTargetType(),
						rmCurrent.getTargetName(), rmCurrent.getIntegrationName(), rmCurrent.getSku(), rmCurrent.getUpc(), rmCurrent.getOperation(),
						rmCurrent.getDescription(), df.format(rmCurrent.getCompletionTime()));
				System.out.println(df.format(rmCurrent.getCompletionTime()));
			}
			

		} catch (Exception exception) {
			exception.getStackTrace();
		} finally {
			csvPrinter.flush();
			csvPrinter.close();
		}

		return file;
	}

	private File buildTemporaryFile() throws IOException {
		return File.createTempFile(FILE_PREFIX + UUID.randomUUID().toString(), FILE_SUFFIX);
	}

}
