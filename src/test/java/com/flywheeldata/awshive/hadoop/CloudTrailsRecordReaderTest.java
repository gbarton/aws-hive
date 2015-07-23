package com.flywheeldata.awshive.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class CloudTrailsRecordReaderTest {
	static TaskAttemptContext context;

	@BeforeClass
	public static void SetupClass() {
		Configuration conf = new Configuration(false);
		conf.set("fs.default.name", "file:///");
		context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

		// RecordReader reader = inputFormat.createRecordReader(split, context);
	}

	@Test
	public void TestBasicInitialize() throws IOException, InterruptedException {

		File testFile = new File(
				"src/test/resources/735407941533_CloudTrail_us-west-2_20150708T2150Z_0luMEQj0VDBc0YyA.json");
		Path path = new Path(testFile.getAbsoluteFile().toURI());
		FileSplit split = new FileSplit(path, 0, testFile.length(), null);

		CloudTrailsRecordReader reader = new CloudTrailsRecordReader();
		reader.initialize(split, context);
	}
}
