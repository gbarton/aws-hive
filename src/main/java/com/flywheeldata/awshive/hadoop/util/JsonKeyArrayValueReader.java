package com.flywheeldata.awshive.hadoop.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.io.SerializedString;

public class JsonKeyArrayValueReader implements Closeable {
	private static final SerializableString SERIALIZED_KEY = new SerializedString("key");
	private static final SerializableString SERIALIZED_VALUE = new SerializedString("value");
	private static final int DEFAULT_BUFFER_SIZE = 8192;

	StringBuilder sb = new StringBuilder(DEFAULT_BUFFER_SIZE);
	JsonFactory factory = new JsonFactory();
	JsonParser parser;
	String jsonKey;

	InputStream in;
	boolean foundEndOfArray;

	int depth = 0;

	public JsonKeyArrayValueReader(InputStream in) throws IOException {
		this.in = in;
		foundEndOfArray = false;
		parser = factory.createParser(in);

		jsonKey = readKey(in);
		findFirstValue();

	}

	private String readKey(InputStream in) throws JsonParseException, IOException {
		while (!parser.nextFieldName(SERIALIZED_KEY)) {
		}

		return readJsonObject(parser, sb);
	}

	public int getRecord(Text record) throws JsonParseException, IOException {

		JsonToken token = parser.nextToken();
		if (foundEndOfArray || token == JsonToken.END_ARRAY) {
			foundEndOfArray = true;
			return 0;
		}

		sb.setLength(0);

		sb.append("{");
		sb.append(jsonKey);
		sb.append(",");
		quoteWrap(sb, "value");
		sb.append(":");
		// sb.append("{");

		readJsonObject(parser, sb);

		sb.append("}");

		record.set(sb.toString());

		return 1;
	}

	public int findFirstValue() throws JsonParseException, IOException {
		while (!parser.nextFieldName(SERIALIZED_VALUE)) {
		}

		if (parser.nextToken() != JsonToken.START_ARRAY) {
			// there are no values, handle it!
			foundEndOfArray = true;
		}

		return 0;
	}

	private String readJsonObject(JsonParser aParser, StringBuilder aStringBuilder) throws IOException {
		if (aParser.getCurrentToken() == JsonToken.START_OBJECT) {
			depth++;
			aStringBuilder.append("{");
		} else if (aParser.getCurrentToken() == JsonToken.FIELD_NAME) {
			quoteWrap(aStringBuilder, aParser.getText());
			aStringBuilder.append(":");
		} else {
			throw new JsonParseException("Expecting either { or a field name to begin a a json object",
					aParser.getCurrentLocation());
		}

		JsonToken lastToken = null;
		do {
			JsonToken token = aParser.nextToken();

			if (token == JsonToken.START_OBJECT) {
				aStringBuilder.append(aParser.getText());
				depth++;
			} else if (token == JsonToken.END_OBJECT) {
				aStringBuilder.append(aParser.getText());
				depth--;
			} else if (token == JsonToken.FIELD_NAME) {
				if (null != lastToken && !(lastToken == JsonToken.START_OBJECT || lastToken == JsonToken.START_ARRAY)) {
					aStringBuilder.append(",");
				}
				quoteWrap(aStringBuilder, aParser.getText());
				aStringBuilder.append(":");
			} else if (token == JsonToken.VALUE_STRING) {
				quoteWrap(aStringBuilder, aParser.getText());
			} else {
				aStringBuilder.append(aParser.getText());
			}

			lastToken = token;

		} while (depth > 0);

		return aStringBuilder.toString();
	}

	@Override
	public void close() throws IOException {
		if (null != parser) {
			parser.close();
		}

		if (null != in)
			in.close();
	}

	private void quoteWrap(StringBuilder sb, String value) {
		sb.append("\"");
		sb.append(value);
		sb.append("\"");
	}

}
