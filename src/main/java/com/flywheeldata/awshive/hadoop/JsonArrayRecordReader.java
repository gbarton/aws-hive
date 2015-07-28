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
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;

import com.flywheeldata.awshive.hadoop.util.JsonArrayReader;

public class JsonArrayRecordReader implements RecordReader<LongWritable, Text> {
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

	public JsonArrayRecordReader(InputSplit split, Configuration job, String anchor)
			throws IOException {
		this.anchor = anchor;
		initialize(split, job);
	}

	public void initialize(InputSplit genericSplit, Configuration job) throws IOException {
		FileSplit split = (FileSplit) genericSplit;

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
	public void close() throws IOException {
		if (null != in) {
			in.close();
		}

	}

	@Override
	public boolean next(LongWritable key, Text value) throws IOException {
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
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
}
