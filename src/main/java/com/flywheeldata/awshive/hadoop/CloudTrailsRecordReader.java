package com.flywheeldata.awshive.hadoop;

import java.io.IOException;
import java.io.InputStream;

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
import org.json.simple.parser.JSONParser;

public class CloudTrailsRecordReader extends RecordReader<LongWritable, Text> {
	long start;
	long end;
	long pos;
	FSDataInputStream fileIn;
	InputStream in;
	boolean isCompressedInput;
	Decompressor decompressor;
	Seekable filePosition;

	JSONParser parser;
	
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

			in = codec.createInputStream(fileIn, decompressor);
			// in = new SplitLineReader(codec.createInputStream(fileIn,
			// decompressor), job, this.recordDelimiterBytes);
			filePosition = fileIn;

		} else {
			fileIn.seek(start);
			in = fileIn;
			// in = new SplitLineReader(fileIn, job, this.recordDelimiterBytes);
			filePosition = fileIn;
		}

		this.pos = start;
		
		parser = new JSONParser();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}
