package com.flywheeldata.awshive.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class JsonKeyArrayValueFileInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		return new JsonKeyArrayValueRecordReader(split, job, "key", "value");
	}

	/**
	 * This is an inherently hierarchical file and cannot be split.
	 * 
	 * @see org.apache.hadoop.mapred.FileInputFormat#isSplitable(org.apache.hadoop.fs.FileSystem,
	 *      org.apache.hadoop.fs.Path)
	 */
	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {

		return false;
	}

}
