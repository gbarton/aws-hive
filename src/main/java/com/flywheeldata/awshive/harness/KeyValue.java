package com.flywheeldata.awshive.harness;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONStreamAware;
import org.json.simple.JSONValue;

public class KeyValue implements JSONStreamAware {
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
		key.setApiVersion("1");
		key.setMethodName("2");
		key.setObjName("3");
		key.setRequestId(4);
		key.setRequestTime(5);

		return key;
	}

	Value generateValue() {
		Value value = new Value();
		value.foo = "bar";

		return value;
	}

	static class Key implements JSONStreamAware {
		Map<String, Object> data;

		public Key() {
			data = new HashMap<String, Object>(5);
		}

		public String getObjName() {
			return (String) data.get("objName");

		}

		public void setObjName(String objName) {
			data.put("objName", objName);
		}

		public String getMethodName() {
			return (String) data.get("methodName");
		}

		public void setMethodName(String methodName) {
			data.put("methodName", methodName);
		}

		public int getRequestTime() {
			return (int) data.get("requestTime");
		}

		public void setRequestTime(int requestTime) {
			data.put("requestTime", requestTime);
		}

		public int getRequestId() {
			return (int) data.get("requestId");
		}

		public void setRequestId(int requestId) {
			data.put("requestId", requestId);
		}

		public String getApiVersion() {
			return (String) data.get("apiVersion");
		}

		public void setApiVersion(String apiVersion) {
			data.put("apiVersion", apiVersion);
		}

		@Override
		public void writeJSONString(Writer arg0) throws IOException {
			JSONValue.writeJSONString(data, arg0);
		}
	}

	static class Value {
		String foo = "";
	}

	@Override
	public void writeJSONString(Writer arg0) throws IOException {
		JSONValue.writeJSONString(pair, arg0);
	}
}
