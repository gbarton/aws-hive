package com.flywheeldata.awshive.hadoop;

import java.io.IOException;
import java.io.Reader;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.flywheeldata.awshive.hadoop.util.JsonArrayReader;

public class JsonArrayRecordReader extends RecordReader<LongWritable, Text> {
	long start;
	long end;
	long pos;
	FSDataInputStream fileIn;
	JsonArrayReader in;
	boolean isCompressedInput;
	Decompressor decompressor;
	Seekable filePosition;
	Reader r;
	Pattern whitespace = Pattern.compile("\\s");

	String anchor;
	
	LongWritable key;
	Text value;
	
	public JsonArrayRecordReader(String anchor) {
		this.anchor = anchor;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();

		// open the file and seek to the start of the split
		final FileSystem fs = file.getFileSystem(job);
		fileIn = fs.open(file);

		CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
		if (null != codec) {
			isCompressedInput = true;
			decompressor = CodecPool.getDecompressor(codec);

			in = new JsonArrayReader(codec.createInputStream(fileIn, decompressor), anchor);
			filePosition = fileIn;

		} else {
			fileIn.seek(start);
			in = new JsonArrayReader(fileIn, anchor);
			filePosition = fileIn;
		}

		start += in.findNextRecord();
		this.pos = start;
	}

	private long getFilePosition() throws IOException {
		long retVal;
		if (isCompressedInput && null != filePosition) {
			retVal = filePosition.getPos();
		} else {
			retVal = pos;
		}
		return retVal;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		int recordSize = 0;

		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Text();
		}

		recordSize = in.getRecord(value);
		pos += recordSize;

		if (recordSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		if (null != in) {
			in.close();
		}

	}
}
