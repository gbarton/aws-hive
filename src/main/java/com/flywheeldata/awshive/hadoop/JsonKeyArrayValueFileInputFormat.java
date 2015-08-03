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
 * "Json Key Array Value File" is a tricky way of saying a json document which
 * has one top-level key element shared by many "value" elements.
 * <br>
 * This InputFormat reads the key then prepends it to each individual value and
 * presents those individual key/value pairs as records. This isn't nearly as
 * effecient from an I/O perspective but it does allow for support of very large
 * files since the key is the only thing held in memory.
 * 
 * @author ksitt_000
 *
 */

public class JsonKeyArrayValueFileInputFormat extends FileInputFormat<LongWritable, Text> {

	private static final String KEY_ELEMENT = "key";
	private static final String VALUE_ELEMENT = "value";

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		return new JsonKeyArrayValueRecordReader(split, job, KEY_ELEMENT, VALUE_ELEMENT);
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
