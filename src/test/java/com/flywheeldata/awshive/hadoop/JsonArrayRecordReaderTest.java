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

public class JsonArrayRecordReaderTest {
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

		JsonArrayRecordReader reader = new JsonArrayRecordReader("eventID");
		reader.initialize(split, context);

		return reader;
	}

	@Test
	public void TestBasicInitialize() throws IOException, InterruptedException {
		JsonArrayRecordReader reader = getReader(
				"src/test/resources/CloudTrails_5.json");
		Assert.assertNotNull(reader);

		if (reader != null) {
			reader.close();
		}
	}

	@Test
	public void TestRead() throws IOException, InterruptedException {
		JsonArrayRecordReader reader = getReader(
				"src/test/resources/CloudTrails_5.json");

		reader.nextKeyValue();
		Assert.assertEquals(12L, reader.getCurrentKey().get());
		String expected1 = "{\"eventID\":e0978682-f1d1-48d6-bc97-8b35f3a5d2b4,\"awsRegion\":\"us-west-2\",\"responseElements\":null,\"sourceIPAddress\":\"127.0.0.1\",\"eventSource\":\"cloudtrail.amazonaws.com\",\"requestParameters\":null,\"userAgent\":\"console.amazonaws.com\",\"userIdentity\":{\"accessKeyId\":\"ID8DS8XB6QZ5SJ8KT4NF1\",\"sessionContext\":{\"attributes\":{\"mfaAuthenticated\":true,\"creationDate\":\"18495704-05-10T13:06Z\"}},\"accountId\":\"-1496388110343125879\",\"principalId\":\"-3274676869669360391\",\"type\":\"Root\",\"arn\":\"arn:aws:iam::-3274676869669360391:root\"},\"eventType\":\"AwsApiCall\",\"requestID\":92679f71-214b-4496-b1c1-d8eedd574734,\"eventTime\":\"37338949-07-29T20:23Z\",\"eventName\":\"DescribeTrails\",\"recipientAccountId\":\"7598508555873132255\"}";
		Assert.assertEquals(expected1, reader.getCurrentValue().toString());

		reader.nextKeyValue();
		Assert.assertEquals(708L, reader.getCurrentKey().get());
		String expected2 = "{\"eventID\":ba7542e9-0d2b-4b73-b602-7e1c8aaa5e4e,\"awsRegion\":\"us-west-2\",\"responseElements\":null,\"sourceIPAddress\":\"127.0.0.1\",\"eventSource\":\"cloudtrail.amazonaws.com\",\"requestParameters\":null,\"userAgent\":\"console.amazonaws.com\",\"userIdentity\":{\"accessKeyId\":\"ID8DS8XB6QZ5SJ8KT4NF1\",\"sessionContext\":{\"attributes\":{\"mfaAuthenticated\":true,\"creationDate\":\"109989109-08-07T16:36Z\"}},\"accountId\":\"5550173425755005064\",\"principalId\":\"8440427140993279071\",\"type\":\"Root\",\"arn\":\"arn:aws:iam::8440427140993279071:root\"},\"eventType\":\"AwsApiCall\",\"requestID\":316d2e43-07d8-4b0a-91e7-89ffb00de989,\"eventTime\":\"107702980-01-19T05:44Z\",\"eventName\":\"DescribeTrails\",\"recipientAccountId\":\"5983484880137438399\"}";
		Assert.assertEquals(expected2, reader.getCurrentValue().toString());
	}
	
	@Test
	public void TestReadAll() throws IOException, InterruptedException {
		JsonArrayRecordReader reader = getReader(
				"src/test/resources/CloudTrails_5.json");
		
		int recordCount = 0;
		while(reader.nextKeyValue()) {
			recordCount++;
		}
		
		Assert.assertEquals(5, recordCount);
	}
}