package com.flywheeldata.awshive.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class JsonKeyArrayValueRecordReaderTest {
	static Configuration conf;
	LongWritable key;
	Text value;

	@BeforeClass
	public static void SetupClass() {
		conf = new Configuration();
		conf.set("fs.default.name", "file:///");
	}

	@Before
	public void Setup() {
		key = new LongWritable();
		value = new Text();
	}

	private JsonKeyArrayValueRecordReader getReader(String filePath) throws IOException, InterruptedException {
		File testFile = new File(filePath);
		Path path = new Path(testFile.getAbsoluteFile().toURI());
		FileSplit split = new FileSplit(path, 0, testFile.length(), new JobConf());

		JsonKeyArrayValueRecordReader reader = new JsonKeyArrayValueRecordReader(split, conf, "key", "value");

		return reader;
	}

	@Test
	public void TestRead() throws IOException, InterruptedException {
		JsonKeyArrayValueRecordReader reader = getReader("src/test/resources/DummyKeyValue_5.json");

		reader.next(key, value);
		//Assert.assertEquals(209L, key.get());
		String expected1 = "{\"key\":{\"requestTime\":2785968492916622342,\"apiVersion\":\"flywheeldata-java-sdk-0.1\",\"requestId\":-654401409116385011,\"methodName\":\"getDummyData\",\"objName\":\"com.flywheeldata.awshive.harness.dummyvalue\"},\"value\":{\"foo\":\"bar\",\"baz\":7571648005999364027}}";
		Assert.assertEquals(expected1, value.toString());

		reader.next(key, value);
		//Assert.assertEquals(708L, key.get());
		String expected2 = "{\"key\":{\"requestTime\":2785968492916622342,\"apiVersion\":\"flywheeldata-java-sdk-0.1\",\"requestId\":-654401409116385011,\"methodName\":\"getDummyData\",\"objName\":\"com.flywheeldata.awshive.harness.dummyvalue\"},\"value\":{\"foo\":\"bar\",\"baz\":-5860456953928048599}}";
		Assert.assertEquals(expected2, value.toString());
	}
	
	@Test
	public void TestReadGz() throws IOException, InterruptedException {
		JsonKeyArrayValueRecordReader reader = getReader("src/test/resources/DummyKeyValue_5.json.gz");

		reader.next(key, value);
		//Assert.assertEquals(209L, key.get());
		String expected1 = "{\"key\":{\"requestTime\":2785968492916622342,\"apiVersion\":\"flywheeldata-java-sdk-0.1\",\"requestId\":-654401409116385011,\"methodName\":\"getDummyData\",\"objName\":\"com.flywheeldata.awshive.harness.dummyvalue\"},\"value\":{\"foo\":\"bar\",\"baz\":7571648005999364027}}";
		Assert.assertEquals(expected1, value.toString());

		reader.next(key, value);
		//Assert.assertEquals(708L, key.get());
		String expected2 = "{\"key\":{\"requestTime\":2785968492916622342,\"apiVersion\":\"flywheeldata-java-sdk-0.1\",\"requestId\":-654401409116385011,\"methodName\":\"getDummyData\",\"objName\":\"com.flywheeldata.awshive.harness.dummyvalue\"},\"value\":{\"foo\":\"bar\",\"baz\":-5860456953928048599}}";
		Assert.assertEquals(expected2, value.toString());
	}

}
