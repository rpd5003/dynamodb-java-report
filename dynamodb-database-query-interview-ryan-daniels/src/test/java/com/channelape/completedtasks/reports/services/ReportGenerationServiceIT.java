package com.channelape.completedtasks.reports.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.UUID;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.BooleanUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.channelape.completedtasks.reports.data.models.DataBusiness;
import com.channelape.completedtasks.reports.data.models.DataChannel;
import com.channelape.completedtasks.reports.data.models.DataCompletedTask;
import com.channelape.completedtasks.reports.data.models.DataIntegration;
import com.channelape.completedtasks.reports.data.models.DataSupplier;
import com.channelape.completedtasks.reports.services.models.Target;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class)
public class ReportGenerationServiceIT {

	private static final Logger LOGGER = LoggerFactory.getLogger(ReportGenerationServiceIT.class);

	private static final String LOCAL_DYNAMODB_ENDPOINT = "http://localhost:8000";

	private static final String RESOURCES_FOLDER = "src/test/resources/";
	private static final String DYNAMODB_TABLES_FOLDER = RESOURCES_FOLDER + "dynamodb/";
	private static final String EXPECTED_RESULTS_FOLDER = RESOURCES_FOLDER + "results/";
	private static final String BUSINESSES_CSV = DYNAMODB_TABLES_FOLDER + "Businesses.csv";
	private static final String CHANNELS_CSV = DYNAMODB_TABLES_FOLDER + "Channels.csv";
	private static final String SUPPLIERS_CSV = DYNAMODB_TABLES_FOLDER + "Suppliers.csv";
	private static final String COMPLETED_TASKS_CSV = DYNAMODB_TABLES_FOLDER + "CompletedTasks.csv";
	private static final String INTEGRATIONS_CSV = DYNAMODB_TABLES_FOLDER + "Integrations.csv";

	private static final String CHARACTER_ENCODING = "UTF-8";
	private static final char CSV_DELIMITER = ',';

	private static DynamoDBProxyServer server;
	private static AmazonDynamoDB amazonDynamoDB;

	@BeforeClass
	public static void beforeClass() throws Exception {
		System.setProperty("sqlite4java.library.path", "target/native-libs");
		final String[] localArgs = { "-inMemory" };
		server = ServerRunner.createServerFromCommandLineArgs(localArgs);
		server.start();
		amazonDynamoDB = new AmazonDynamoDBClient(new BasicAWSCredentials("", ""));
		amazonDynamoDB.setEndpoint(LOCAL_DYNAMODB_ENDPOINT);

		final DynamoDBMapper dynamoDBMapper = new DynamoDBMapper(amazonDynamoDB);

		createTable(dynamoDBMapper, DataBusiness.class);
		createTable(dynamoDBMapper, DataChannel.class);
		createTable(dynamoDBMapper, DataCompletedTask.class);
		createTable(dynamoDBMapper, DataIntegration.class);
		createTable(dynamoDBMapper, DataSupplier.class);

		final CSVParser businessesCsvParser = buildCsvParser(BUSINESSES_CSV);
		businessesCsvParser.getRecords().parallelStream().skip(1).map(ReportGenerationServiceIT::mapDataBusiness)
				.forEach(dataBusiness -> dynamoDBMapper.save(dataBusiness));
		businessesCsvParser.close();

		final CSVParser channelsCsvParser = buildCsvParser(CHANNELS_CSV);
		channelsCsvParser.getRecords().parallelStream().skip(1).map(ReportGenerationServiceIT::mapDataChannel)
				.forEach(dataBusiness -> dynamoDBMapper.save(dataBusiness));
		channelsCsvParser.close();

		final CSVParser suppliersCsvParser = buildCsvParser(SUPPLIERS_CSV);
		suppliersCsvParser.getRecords().parallelStream().skip(1).map(ReportGenerationServiceIT::mapDataSupplier)
				.forEach(dataBusiness -> dynamoDBMapper.save(dataBusiness));
		suppliersCsvParser.close();

		final CSVParser completedTasksCsvParser = buildCsvParser(COMPLETED_TASKS_CSV);
		completedTasksCsvParser.getRecords().parallelStream().skip(1)
				.map(ReportGenerationServiceIT::mapDataCompletedTask)
				.forEach(dataBusiness -> dynamoDBMapper.save(dataBusiness));
		completedTasksCsvParser.close();

		final CSVParser integrationsCsvParser = buildCsvParser(INTEGRATIONS_CSV);
		integrationsCsvParser.getRecords().parallelStream().skip(1).map(ReportGenerationServiceIT::mapDataIntegration)
				.forEach(dataBusiness -> dynamoDBMapper.save(dataBusiness));
		integrationsCsvParser.close();
	}

	@Autowired
	private ReportGenerationService reportGenerationService;

	@Configuration
	@ComponentScan(basePackageClasses = { ReportGenerationService.class })
	static class ContextConfiguration {
		@Bean
		public DynamoDBMapper dynamoDBMapper() {
			return new DynamoDBMapper(amazonDynamoDB);
		}
	}

	@Test
	public void givenEarliestTaskCompletionTimeOfJuly2016AndAndLatestTaskCompletionTimeOfAugust2016AndChannelTargetWithNonExistentIdWhenGeneratingReportThenReturnEmptyFileWithHeaders()
			throws IOException {
		final DateTime earliestTaskCompletionTime = DateTime.parse("2016-07-01T00:00:00.000Z");
		final DateTime latestTaskCompletionTime = DateTime.parse("2016-08-01T00:00:00.000Z");
		final Target target = new Target(Target.Type.CHANNEL, UUID.randomUUID().toString());

		final File actualFile = reportGenerationService.generate(earliestTaskCompletionTime, latestTaskCompletionTime,
				target);

		final List<CSVRecord> actualRecords = getActualRecords(actualFile);
		assertHeadersRow(actualRecords);
		assertEquals(1, actualRecords.size());
	}

	@Test
	public void givenEarliestTaskCompletionTimeOfJuly2016AndAndLatestTaskCompletionTimeOfAugust2016AndBusinessTargetWithNonExistentIdWhenGeneratingReportThenReturnEmptyFileWithHeaders()
			throws IOException {
		final DateTime earliestTaskCompletionTime = DateTime.parse("2016-07-01T00:00:00.000Z");
		final DateTime latestTaskCompletionTime = DateTime.parse("2016-08-01T00:00:00.000Z");
		final Target target = new Target(Target.Type.BUSINESS, UUID.randomUUID().toString());

		final File actualFile = reportGenerationService.generate(earliestTaskCompletionTime, latestTaskCompletionTime,
				target);

		final List<CSVRecord> actualRecords = getActualRecords(actualFile);
		assertHeadersRow(actualRecords);
		assertEquals(1, actualRecords.size());
	}

	@Test
	public void givenEarliestTaskCompletionTimeOfJuly2016AndAndLatestTaskCompletionTimeOfAugust2016AndSupplierTargetWithNonExistentIdWhenGeneratingReportThenReturnEmptyFileWithHeaders()
			throws IOException {
		final DateTime earliestTaskCompletionTime = DateTime.parse("2016-07-01T00:00:00.000Z");
		final DateTime latestTaskCompletionTime = DateTime.parse("2016-08-01T00:00:00.000Z");
		final Target target = new Target(Target.Type.SUPPLIER, UUID.randomUUID().toString());

		final File actualFile = reportGenerationService.generate(earliestTaskCompletionTime, latestTaskCompletionTime,
				target);

		final List<CSVRecord> actualRecords = getActualRecords(actualFile);
		assertHeadersRow(actualRecords);
		assertEquals(1, actualRecords.size());
	}

	@Test
	public void givenEarliestTaskCompletionTimeOfFifteenWeeksAgoAndAndLatestTaskCompletionTimeOfFourYearsAgoAndSupplierTargetWithExistentIdWhenGeneratingReportThenReturnEmptyFileWithHeaders()
			throws IOException {
		final DateTime earliestTaskCompletionTime = DateTime.now(DateTimeZone.UTC).minusWeeks(15);
		final DateTime latestTaskCompletionTime = DateTime.now(DateTimeZone.UTC).minusYears(4);
		final String supplierId = "9b5aaa6b-7740-488c-b965-9b89a40bc0dd";
		final Target target = new Target(Target.Type.SUPPLIER, supplierId);

		final File actualFile = reportGenerationService.generate(earliestTaskCompletionTime, latestTaskCompletionTime,
				target);

		final List<CSVRecord> actualRecords = getActualRecords(actualFile);
		assertHeadersRow(actualRecords);
		assertEquals(1, actualRecords.size());
	}

	@Test
	public void givenEarliestTaskCompletionTimeOfJanuary2005AndAndLatestTaskCompletionTimeOfNovember2017AndBusinessTargetWithExistentIdWhenGeneratingReportThenReturnFileWithExpectedRows()
			throws IOException {
		final DateTime earliestTaskCompletionTime = DateTime.parse("2005-01-01T00:00:00.000Z");
		final DateTime latestTaskCompletionTime = DateTime.parse("2017-11-01T00:00:00.000Z");
		final String businessId = "94414666-68bd-46ff-95e9-a3ff98ca15fc";
		final Target target = new Target(Target.Type.BUSINESS, businessId);

		final File actualFile = reportGenerationService.generate(earliestTaskCompletionTime, latestTaskCompletionTime,
				target);

		final List<CSVRecord> actualRecords = getActualRecords(actualFile);
		final String expectedFileName = "business_query.csv";
		assertRecords(expectedFileName, actualRecords);
	}

	@Test
	public void givenEarliestTaskCompletionTimeOfJuly2016AndAndLatestTaskCompletionTimeOfAugust2016AndBusinessTargetWithExistentIdAndChannelTargetWithExistentIdWhenGeneratingReportThenReturnFileWithExpectedRows()
			throws IOException {
		final DateTime earliestTaskCompletionTime = DateTime.parse("2016-07-01T00:00:00.000Z");
		final DateTime latestTaskCompletionTime = DateTime.parse("2016-08-01T00:00:00.000Z");
		final String channelId = "2ba1f000-5daa-441a-b20e-20befa32518c";
		final Target target = new Target(Target.Type.CHANNEL, channelId);

		final File actualFile = reportGenerationService.generate(earliestTaskCompletionTime, latestTaskCompletionTime,
				target);

		final List<CSVRecord> actualRecords = getActualRecords(actualFile);
		final String expectedFileName = "channel_query.csv";
		assertRecords(expectedFileName, actualRecords);
	}

	@Test
	public void givenEarliestTaskCompletionTimeOfApril2012AndAndLatestTaskCompletionTimeOfApril2017AndBusinessTargetWithExistentIdAndChannelTargetWithExistentIdWhenGeneratingReportThenReturnFileWithExpectedRows()
			throws IOException {
		final DateTime earliestTaskCompletionTime = DateTime.parse("2012-04-01T00:00:00.000Z");
		final DateTime latestTaskCompletionTime = DateTime.parse("2017-04-01T00:00:00.000Z");
		final String supplierId = "9b5aaa6b-7740-488c-b965-9b89a40bc0dd";
		final Target target = new Target(Target.Type.SUPPLIER, supplierId);

		final File actualFile = reportGenerationService.generate(earliestTaskCompletionTime, latestTaskCompletionTime,
				target);

		final List<CSVRecord> actualRecords = getActualRecords(actualFile);
		final String expectedFileName = "supplier_query.csv";
		assertRecords(expectedFileName, actualRecords);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		server.stop();
	}

	private static <T> void createTable(final DynamoDBMapper dynamoDBMapper, final Class<T> tableClass)
			throws InterruptedException {
		final CreateTableRequest createTableRequest = dynamoDBMapper.generateCreateTableRequest(tableClass);
		final List<GlobalSecondaryIndex> globalSecondaryIndexes = createTableRequest.getGlobalSecondaryIndexes();

		final ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput();
		provisionedThroughput.setReadCapacityUnits(1000L);
		provisionedThroughput.setWriteCapacityUnits(1000L);
		createTableRequest.setProvisionedThroughput(provisionedThroughput);

		if (globalSecondaryIndexes != null) {
			for (GlobalSecondaryIndex globalSecondaryIndex : globalSecondaryIndexes) {
				globalSecondaryIndex.setProvisionedThroughput(provisionedThroughput);
				final Projection projection = new Projection();
				projection.setProjectionType(ProjectionType.ALL);
				globalSecondaryIndex.setProjection(projection);
			}
		}

		final CreateTableResult createTableResult = amazonDynamoDB.createTable(createTableRequest);
		final String tableName = createTableResult.getTableDescription().getTableName();

		while (!amazonDynamoDB.listTables().getTableNames().contains(tableName)) {
			LOGGER.info(String.format("Waiting for %s table to become available.", tableName));
			Thread.sleep(200);
		}
	}

	private static CSVParser buildCsvParser(final String path) throws IOException {
		final File csvfile = new File(path);
		return buildCsvParser(csvfile);
	}

	private static CSVParser buildCsvParser(final File csvfile)
			throws UnsupportedEncodingException, FileNotFoundException, IOException {
		final Reader reader = new InputStreamReader(new FileInputStream(csvfile), CHARACTER_ENCODING);
		final CSVFormat csvFormat = CSVFormat.RFC4180.withDelimiter(CSV_DELIMITER).withIgnoreEmptyLines();
		final CSVParser csvParser = new CSVParser(reader, csvFormat);
		return csvParser;
	}

	private static DataBusiness mapDataBusiness(final CSVRecord csvRecord) {
		final DataBusiness dataBusiness = new DataBusiness();
		dataBusiness.setId(csvRecord.get(0));
		dataBusiness.setAlphabeticCurrencyCode(csvRecord.get(1));
		dataBusiness.setInventoryItemKey(csvRecord.get(2));
		dataBusiness.setName(csvRecord.get(3));
		dataBusiness.setTimeZone(csvRecord.get(4));
		return dataBusiness;
	}

	private static DataChannel mapDataChannel(final CSVRecord csvRecord) {
		final DataChannel dataChannel = new DataChannel();
		dataChannel.setId(csvRecord.get(0));
		dataChannel.setBusinessId(csvRecord.get(1));
		dataChannel.setCreationTimeInMilliseconds(Long.valueOf(csvRecord.get(2)));
		dataChannel.setEnabled(mapBoolean(csvRecord.get(3)));
		dataChannel.setIntegrationId(csvRecord.get(4));
		dataChannel.setName(csvRecord.get(5));
		dataChannel.setUpdateTimeInMilliseconds(Long.valueOf(csvRecord.get(6)));
		return dataChannel;
	}

	private static DataSupplier mapDataSupplier(final CSVRecord csvRecord) {
		final DataSupplier dataSupplier = new DataSupplier();
		dataSupplier.setId(csvRecord.get(0));
		dataSupplier.setBusinessId(csvRecord.get(1));
		dataSupplier.setCreationTimeInMilliseconds(Long.valueOf(csvRecord.get(2)));
		dataSupplier.setEnabled(mapBoolean(csvRecord.get(3)));
		dataSupplier.setIntegrationId(csvRecord.get(4));
		dataSupplier.setName(csvRecord.get(5));
		dataSupplier.setUpdateTimeInMilliseconds(Long.valueOf(csvRecord.get(6)));
		return dataSupplier;
	}

	private static DataCompletedTask mapDataCompletedTask(final CSVRecord csvRecord) {
		final DataCompletedTask dataCompletedTask = new DataCompletedTask();
		dataCompletedTask.setId(csvRecord.get(0));
		dataCompletedTask.setBusinessId(csvRecord.get(1));
		dataCompletedTask.setCompletionTimeInMilliseconds(Long.valueOf(csvRecord.get(2)));
		dataCompletedTask.setDescription(csvRecord.get(3));
		dataCompletedTask.setOperation(csvRecord.get(4));
		dataCompletedTask.setSku(csvRecord.get(5));
		dataCompletedTask.setTargetId(csvRecord.get(6));
		dataCompletedTask.setTargetType(csvRecord.get(7));
		dataCompletedTask.setUpc(csvRecord.get(8));
		return dataCompletedTask;
	}

	private static DataIntegration mapDataIntegration(final CSVRecord csvRecord) {
		final DataIntegration dataIntegration = new DataIntegration();
		dataIntegration.setId(csvRecord.get(0));
		dataIntegration.setCreationTimeInMilliseconds(Long.valueOf(csvRecord.get(1)));
		dataIntegration.setPublic(mapBoolean(csvRecord.get(2)));
		dataIntegration.setName(csvRecord.get(3));
		dataIntegration.setType(csvRecord.get(4));
		dataIntegration.setUpdateTimeInMilliseconds(Long.valueOf(csvRecord.get(5)));
		return dataIntegration;
	}

	private static boolean mapBoolean(final String value) {
		return BooleanUtils.toBooleanObject(Integer.valueOf(value));
	}

	private List<CSVRecord> getActualRecords(final File actualFile) throws IOException {
		assertNotNull(actualFile);
		final CSVParser csvParser = buildCsvParser(actualFile);
		final List<CSVRecord> actualRecords = csvParser.getRecords();
		return actualRecords;
	}

	private List<CSVRecord> getExpectedRecords(final String expectedFileName) throws IOException {
		final CSVParser csvParser = buildCsvParser(EXPECTED_RESULTS_FOLDER + expectedFileName);
		final List<CSVRecord> actualRecords = csvParser.getRecords();
		return actualRecords;
	}

	private void assertHeadersRow(final List<CSVRecord> actualRecords) {
		assertFalse("File should not be empty.", actualRecords.isEmpty());
		final List<String> expectedHeaders = ReportGenerationService.HEADERS;
		final CSVRecord actualHeaderRecord = actualRecords.get(0);
		assertEquals("Expected a different number of headers.", expectedHeaders.size(), actualHeaderRecord.size());
		for (int i = 0; i < actualHeaderRecord.size(); i++) {
			assertEquals(String.format("Expected a different header at column %s", (i + 1)), expectedHeaders.get(i),
					actualHeaderRecord.get(i));
		}
	}

	private void assertRecords(final String expectedFileName, final List<CSVRecord> actualRecords) throws IOException {
		final List<CSVRecord> expectedRecords = getExpectedRecords(expectedFileName);

		assertEquals(String.format("Expected a different number of records. %s does not match.", expectedFileName),
				expectedRecords.size(), actualRecords.size());

		for (int i = 0; i < expectedRecords.size(); i++) {
			final CSVRecord expectedRecord = expectedRecords.get(i);
			final CSVRecord actualRecord = actualRecords.get(i);

			assertEquals(String.format(
					"Record at index %s has a different number of columns then expected. %s does not match.", (i + 1),
					expectedFileName), expectedRecord.size(), actualRecord.size());
			for (int j = 0; j < expectedRecord.size(); j++) {
				final String expectedRecordValue = expectedRecord.get(j);
				final String actualRecordValue = actualRecord.get(j);
				assertEquals(String.format("Expected different cell value for column %s and row %s. %s does not match.",
						(i + 1), (j + 1), expectedFileName), expectedRecordValue, actualRecordValue);
			}
		}
	}

}
