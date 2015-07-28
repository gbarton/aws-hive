package com.flywheeldata.awshive.harness;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.json.simple.JSONStreamAware;
import org.json.simple.JSONValue;

public class KeyValue implements JSONStreamAware {
	static final String DEFAULT_OBJ_NAME = "com.flywheeldata.awshive.harness.dummyvalue";
	static final String DEFAULT_METHOD_NAME = "getDummyData";
	static final String DEFAULT_API_VERSION = "flywheeldata-java-sdk-0.1";

	Random r = new Random();

	Map<String, Object> pair;
	List<Value> values;

	public KeyValue() {
		pair = new LinkedHashMap<String, Object>();

		Key key = generateKey();
		pair.put("key", key);

		values = new ArrayList<Value>();
		pair.put("value", values);
	}

	public void addValue() {
		Value value = generateValue();
		values.add(value);
	}

	Key generateKey() {
		Key key = new Key();
		key.setApiVersion(DEFAULT_API_VERSION);
		key.setMethodName(DEFAULT_METHOD_NAME);
		key.setObjName(DEFAULT_OBJ_NAME);
		key.setRequestId(r.nextLong());
		key.setRequestTime(r.nextLong());

		return key;
	}

	Value generateValue() {
		Value value = new Value();
		value.put("foo", "bar");
		value.put("baz", r.nextLong());

		return value;
	}

	static class BaseStore implements JSONStreamAware {
		Map<String, Object> data = new HashMap<String, Object>();

		@Override
		public void writeJSONString(Writer arg0) throws IOException {
			JSONValue.writeJSONString(data, arg0);
		}

		public void put(String key, Object value) {
			data.put(key, value);
		}

		public Object get(String key) {
			return data.get(key);
		}
	}

	static class Key extends BaseStore {
		public String getObjName() {
			return (String) get("objName");

		}

		public void setObjName(String objName) {
			put("objName", objName);
		}

		public String getMethodName() {
			return (String) get("methodName");
		}

		public void setMethodName(String methodName) {
			put("methodName", methodName);
		}

		public long getRequestTime() {
			return (long) get("requestTime");
		}

		public void setRequestTime(long requestTime) {
			put("requestTime", requestTime);
		}

		public long getRequestId() {
			return (long) get("requestId");
		}

		public void setRequestId(long requestId) {
			put("requestId", requestId);
		}

		public String getApiVersion() {
			return (String) get("apiVersion");
		}

		public void setApiVersion(String apiVersion) {
			data.put("apiVersion", apiVersion);
		}
	}

	static class Value extends BaseStore {

	}

	@Override
	public void writeJSONString(Writer arg0) throws IOException {
		JSONValue.writeJSONString(pair, arg0);
	}
}
