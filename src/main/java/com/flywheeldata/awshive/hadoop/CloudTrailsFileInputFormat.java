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

/**
 * Read json records contained within an array. This is a little tricky because
 * it needs to fulfill three requirements: able to handle large files, fast, and
 * splittable.
 * <br>
 * In order to handle splits we've got to assume that the reader can be
 * initialized anywhere within the json file. To address this we use "anchors".
 * An anchor is a string of text that only & always appears in the top level of
 * a record, sort of like a primary key in a database.
 * 
 * For CloudTrails logs that string is "eventID", every event has one but they
 * never appear elsewhere in the file.
 * 
 * @author ksitt_000
 *
 */
public class CloudTrailsFileInputFormat extends FileInputFormat<LongWritable, Text> {
	private static final String CLOUDTRAILS_ANCHOR = "eventID";

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return super.isSplitable(fs, filename);
	}

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
			throws IOException {

		return new JsonArrayRecordReader(split, job, CLOUDTRAILS_ANCHOR);

	}

}
