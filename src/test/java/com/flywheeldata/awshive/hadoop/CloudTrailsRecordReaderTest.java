package com.flywheeldata.awshive.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CloudTrailsRecordReaderTest {
	static TaskAttemptContext context;

	@BeforeClass
	public static void SetupClass() {
		Configuration conf = new Configuration(false);
		conf.set("fs.default.name", "file:///");
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
	}

	private JsonArrayRecordReader getReader(String filePath) throws IOException, InterruptedException {
		File testFile = new File(filePath);
		Path path = new Path(testFile.getAbsoluteFile().toURI());
		FileSplit split = new FileSplit(path, 0, testFile.length(), null);

		JsonArrayRecordReader reader = new JsonArrayRecordReader();
		reader.initialize(split, context);

		return reader;
	}

	@Test
	public void TestBasicInitialize() throws IOException, InterruptedException {
		JsonArrayRecordReader reader = getReader(
				"src/test/resources/735407941533_CloudTrail_us-west-2_20150708T2150Z_0luMEQj0VDBc0YyA.json");
		Assert.assertNotNull(reader);

		if (reader != null) {
			reader.close();
		}
	}

	@Test
	public void TestRead() throws IOException, InterruptedException {
		JsonArrayRecordReader reader = getReader(
				"src/test/resources/735407941533_CloudTrail_us-west-2_20150708T2150Z_0luMEQj0VDBc0YyA.json");

		reader.nextKeyValue();
		Assert.assertEquals(12L, reader.getCurrentKey().get());
		String expected1 = "{\"eventVersion\":\"1.03\",\"userIdentity\":{\"type\":\"Root\",\"principalId\":\"735407941533\",\"arn\":\"arn:aws:iam::735407941533:root\",\"accountId\":\"735407941533\",\"accessKeyId\":\"ASIAIU2PXAOEHABDY25Q\",\"sessionContext\":{\"attributes\":{\"mfaAuthenticated\":\"false\",\"creationDate\":\"2015-07-08T21:42:25Z\"}}},\"eventTime\":\"2015-07-08T21:45:09Z\",\"eventSource\":\"cloudtrail.amazonaws.com\",\"eventName\":\"DescribeTrails\",\"awsRegion\":\"us-west-2\",\"sourceIPAddress\":\"96.244.144.37\",\"userAgent\":\"console.amazonaws.com\",\"requestParameters\":{\"trailNameList\":[]},\"responseElements\":null,\"requestID\":\"9a9e2be4-25ba-11e5-8b00-a990a47d65f9\",\"eventID\":\"16478378-9e55-4093-a0e9-7ff223de9ee3\",\"eventType\":\"AwsApiCall\",\"recipientAccountId\":\"735407941533\"}";
		Assert.assertEquals(expected1, reader.getCurrentValue().toString());
		
		reader.nextKeyValue();
		Assert.assertEquals(723L, reader.getCurrentKey().get());
		String expected2 = "{\"eventVersion\":\"1.03\",\"userIdentity\":{\"type\":\"Root\",\"principalId\":\"735407941533\",\"arn\":\"arn:aws:iam::735407941533:root\",\"accountId\":\"735407941533\",\"accessKeyId\":\"ASIAIU2PXAOEHABDY25Q\",\"sessionContext\":{\"attributes\":{\"mfaAuthenticated\":\"false\",\"creationDate\":\"2015-07-08T21:42:25Z\"}}},\"eventTime\":\"2015-07-08T21:44:25Z\",\"eventSource\":\"cloudtrail.amazonaws.com\",\"eventName\":\"DescribeTrails\",\"awsRegion\":\"us-west-2\",\"sourceIPAddress\":\"96.244.144.37\",\"userAgent\":\"console.amazonaws.com\",\"requestParameters\":{\"trailNameList\":[]},\"responseElements\":null,\"requestID\":\"807fc4c9-25ba-11e5-8b00-a990a47d65f9\",\"eventID\":\"8c03e8d5-e313-49be-a28d-d0648900147d\",\"eventType\":\"AwsApiCall\",\"recipientAccountId\":\"735407941533\"}";
		Assert.assertEquals(expected2, reader.getCurrentValue().toString());
	}
}