package com.flywheeldata.awshive.hadoop;

import java.io.IOException;
import java.io.Reader;

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

import com.flywheeldata.awshive.hadoop.util.JsonKeyArrayValueReader;

public class JsonKeyArrayValueRecordReader implements RecordReader<LongWritable, Text> {

	long start;
	long end;
	long pos;
	FSDataInputStream fileIn;
	JsonKeyArrayValueReader in;
	boolean isCompressedInput;
	Decompressor decompressor;
	Seekable filePosition;
	Reader r;

	String keyElement;
	String valueElement;

	String keyElementValue;

	LongWritable key;
	Text value;

	public JsonKeyArrayValueRecordReader(InputSplit split, Configuration job, String keyElement, String valueElement)
			throws IOException {
		initialize(split, job);
	}

	private void initialize(InputSplit genericSplit, Configuration job) throws IOException {
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

			in = new JsonKeyArrayValueReader(codec.createInputStream(fileIn, decompressor));
			filePosition = fileIn;

		} else {
			fileIn.seek(start);
			in = new JsonKeyArrayValueReader(fileIn);
			filePosition = fileIn;
		}

		// start += in.findNextRecord();
		this.pos = start;

	}

	@Override
	public boolean next(LongWritable key, Text value) throws IOException {
		int recordSize = 0;

		if (key == null) {
			key = createKey();
		}
		key.set(pos);

		if (value == null) {
			value = createValue();
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
	public void close() throws IOException {
		if (null != in) {
			in.close();
		}
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

}
