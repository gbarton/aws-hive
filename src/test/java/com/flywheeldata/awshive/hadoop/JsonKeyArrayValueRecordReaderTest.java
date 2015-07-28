package com.flywheeldata.awshive.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class JsonKeyArrayValueRecordReaderTest {
	static TaskAttemptContext context;

	@BeforeClass
	public static void SetupClass() {
		Configuration conf = new Configuration(false);
		conf.set("fs.default.name", "file:///");
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
	}

	private JsonKeyArrayValueRecordReader getReader(String filePath) throws IOException, InterruptedException {
		File testFile = new File(filePath);
		Path path = new Path(testFile.getAbsoluteFile().toURI());
		FileSplit split = new FileSplit(path, 0, testFile.length(), null);

		JsonKeyArrayValueRecordReader reader = new JsonKeyArrayValueRecordReader();
		reader.initialize(split, context);

		return reader;
	}

	//@Test
	public void TestRead() throws IOException, InterruptedException {
		JsonKeyArrayValueRecordReader reader = getReader("src/test/resources/DummyKeyValue_5.json");

		reader.nextKeyValue();
		Assert.assertEquals(209L, reader.getCurrentKey().get());
		String expected1 = "{\"key\":{\"requestTime\":2785968492916622342,\"apiVersion\":\"flywheeldata-java-sdk-0.1\",\"requestId\":-654401409116385011,\"methodName\":\"getDummyData\",\"objName\":\"com.flywheeldata.awshive.harness.dummyvalue\"},\"value\":{\"foo\":\"bar\",\"baz\":7571648005999364027}}";
		Assert.assertEquals(expected1, reader.getCurrentValue().toString());

		reader.nextKeyValue();
		Assert.assertEquals(708L, reader.getCurrentKey().get());
		String expected2 = "{\"key\":{\"requestTime\":2785968492916622342,\"apiVersion\":\"flywheeldata-java-sdk-0.1\",\"requestId\":-654401409116385011,\"methodName\":\"getDummyData\",\"objName\":\"com.flywheeldata.awshive.harness.dummyvalue\"},\"value\":{\"foo\":\"bar\",\"baz\":-5860456953928048599}}";
		Assert.assertEquals(expected2, reader.getCurrentValue().toString());
	}

}
