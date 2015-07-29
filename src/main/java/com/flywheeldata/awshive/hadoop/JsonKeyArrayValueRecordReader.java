package com.flywheeldata.awshive.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

public class JsonKeyArrayValueRecordReader implements RecordReader<LongWritable, Text> {

	@Override
	public boolean next(LongWritable key, Text value) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public LongWritable createKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Text createValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

}
