package com.flywheeldata.awshive.hadoop.util;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Stack;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonArrayReader implements Closeable {
	private static final int DEFAULT_BUFFER_SIZE = 8192;
	public static final Logger LOG = LoggerFactory.getLogger(JsonArrayReader.class);

	int bufferSize;
	InputStream in;
	String anchor;
	byte[] buffer;
	boolean foundEndOfArray = false;

	int bufferLength = 0;
	int bufferPosition = 0;

	public JsonArrayReader(InputStream in, String anchor) {
		this(in, anchor, DEFAULT_BUFFER_SIZE);
	}

	public JsonArrayReader(InputStream in, String anchor, int bufferSize) {
		this.in = in;
		this.anchor = anchor;
		this.bufferSize = bufferSize;
		this.buffer = new byte[this.bufferSize];
	}

	public int getRecord(Text str) throws IOException {
		if (foundEndOfArray) {
			return 0;
		}

		str.clear();

		int charsRead = 0;
		int currentChar;
		Stack<Integer> bracesStack = new Stack<Integer>();

		int start = bufferPosition;

		do {
			if (bufferPosition >= bufferLength) {
				str.append(buffer, start, bufferPosition);

				bufferLength = fillBuffer();
				start = 0;
				bufferPosition = 0;
				if (bufferLength <= 0)
					throw new EOFException("File ended with an incomplete record");
			}

			currentChar = buffer[bufferPosition];
			charsRead++;
			bufferPosition++;

			if (currentChar == '{')
				bracesStack.push(charsRead);
			else if (currentChar == '}')
				bracesStack.pop();

			if (bracesStack.isEmpty()) {
				str.append(buffer, start, bufferPosition - start);
				int charsToNextRecord = advanceToNext();
				return charsRead + charsToNextRecord;
			}
		} while (true);
	}

	public int findNextRecord() throws IOException {
		Stack<Integer> bracesStack = new Stack<Integer>();
		char pattern[] = anchor.toCharArray();

		int currentChar;
		int offsetInPattern = 0;

		do {
			if (bufferPosition >= bufferLength) {
				bufferLength = fillBuffer();
				bufferPosition = 0;
				if (bufferLength <= 0)
					throw new EOFException("Could not find record within file");
			}

			currentChar = buffer[bufferPosition];

			if (currentChar == '{')
				bracesStack.push(bufferPosition);
			else if (currentChar == '}' && !bracesStack.empty())
				bracesStack.pop();

			if (currentChar == pattern[offsetInPattern] && !bracesStack.empty()) {
				offsetInPattern++;
				if (offsetInPattern == pattern.length) {
					int recordStart = bracesStack.pop();
					bufferPosition = recordStart;
					return recordStart;
				}
			} else
				offsetInPattern = 0;

			bufferPosition++;
		} while (true);
	}

	private int advanceToNext() throws IOException {

		int charsRead = 0;
		int currentChar;

		do {
			if (bufferPosition >= bufferLength) {
				bufferLength = fillBuffer();
				bufferPosition = 0;
				if (bufferLength <= 0)
					throw new EOFException("Invalid JSON encountered");
			}

			currentChar = buffer[bufferPosition];
			bufferPosition++;
			charsRead++;

			if (!Character.isWhitespace(currentChar)) {
				if (currentChar == ',') {
					// okay, read to read the next object in the array
					return charsRead;
				} else if (currentChar == ']') {
					// we just read the last item in the array
					foundEndOfArray = true;
					return charsRead;
				} else {
					// something has gone horribly, horribly wrong
					throw new IOException("Invalid JSON encountered");
				}
			}

		} while (true);
	}

	protected int fillBuffer() throws IOException {
		return in.read(buffer);
	}

	public void close() throws IOException {
		if (null != in) {
			in.close();
		}

	}

}
