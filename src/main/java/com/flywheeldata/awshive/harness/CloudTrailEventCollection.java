package com.flywheeldata.awshive.harness;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang.reflect.FieldUtils;
import org.json.simple.JSONStreamAware;
import org.json.simple.JSONValue;

import com.amazonaws.services.cloudtrail.processinglibrary.model.CloudTrailEvent;
import com.amazonaws.services.cloudtrail.processinglibrary.model.CloudTrailEventData;
import com.amazonaws.services.cloudtrail.processinglibrary.model.internal.CloudTrailDataStore;
import com.amazonaws.services.cloudtrail.processinglibrary.model.internal.CloudTrailEventField;
import com.amazonaws.services.cloudtrail.processinglibrary.model.internal.SessionContext;
import com.amazonaws.services.cloudtrail.processinglibrary.model.internal.UserIdentity;

public class CloudTrailEventCollection implements JSONStreamAware {
	private static final String ROOT_TYPE = "Root";
	private static final String ACCESS_KEY_ID = "ID8DS8XB6QZ5SJ8KT4NF1";
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
	private static final String EVENT_SOURCE = "cloudtrail.amazonaws.com";
	private static final String EVENT_NAME = "DescribeTrails";
	private static final String AWS_REGION = "us-west-2";
	private static final String SOURCE_IP_ADDRESS = "127.0.0.1";
	private static final String USER_AGENT = "console.amazonaws.com";
	private static final String EVENT_TYPE = "AwsApiCall";

	protected List<CloudTrailEvent> events;
	Random r = new Random();

	public CloudTrailEventCollection() {
		this(0);
	}

	public CloudTrailEventCollection(int numEvents) {
		events = new ArrayList<CloudTrailEvent>();
	}

	public void GenerateEvent() throws IOException {
		CloudTrailEventData cted = new CloudTrailEventData();

		cted.add(CloudTrailEventField.eventID.toString(), UUID.randomUUID());
		cted.add(CloudTrailEventField.userIdentity.toString(), generateUserIdentity());
		cted.add(CloudTrailEventField.eventTime.toString(), DATE_FORMAT.format(new Date(r.nextLong())));
		cted.add(CloudTrailEventField.eventSource.toString(), EVENT_SOURCE);
		cted.add(CloudTrailEventField.eventName.toString(), EVENT_NAME);
		cted.add(CloudTrailEventField.awsRegion.toString(), AWS_REGION);
		cted.add(CloudTrailEventField.eventName.toString(), EVENT_NAME);
		cted.add(CloudTrailEventField.sourceIPAddress.toString(), SOURCE_IP_ADDRESS);
		cted.add(CloudTrailEventField.userAgent.toString(), USER_AGENT);
		cted.add(CloudTrailEventField.requestParameters.toString(), null);
		cted.add(CloudTrailEventField.responseElements.toString(), null);
		cted.add(CloudTrailEventField.requestID.toString(), UUID.randomUUID());
		cted.add(CloudTrailEventField.eventType.toString(), EVENT_TYPE);
		cted.add(CloudTrailEventField.recipientAccountId.toString(), Long.toString(r.nextLong()));

		CloudTrailEvent cte = new CloudTrailEvent(cted, null);
		events.add(cte);
	}

	private UserIdentity generateUserIdentity() {
		UserIdentity userIdentity = new UserIdentity();

		userIdentity.add(CloudTrailEventField.type.toString(), ROOT_TYPE);
		userIdentity.add(CloudTrailEventField.principalId.toString(), Long.toString(r.nextLong()));
		userIdentity.add(CloudTrailEventField.arn.toString(),
				"arn:aws:iam::" + userIdentity.getPrincipalId() + ":" + "root");
		userIdentity.add(CloudTrailEventField.accountId.toString(), Long.toString(r.nextLong()));
		userIdentity.add(CloudTrailEventField.accessKeyId.toString(), ACCESS_KEY_ID);

		SessionContext sessionContext = new SessionContext();
		Map<String, Object> sessionContextAttributes = new HashMap<String, Object>();
		sessionContextAttributes.put("mfaAuthenticated", r.nextBoolean());
		sessionContextAttributes.put("creationDate", DATE_FORMAT.format(new Date(r.nextLong())));
		sessionContext.add("attributes", sessionContextAttributes);
		userIdentity.add(CloudTrailEventField.sessionContext.toString(), sessionContext);

		return userIdentity;
	}

	public void writeJSONString(Writer arg0) throws IOException {
		List<Map<String, Object>> records = new ArrayList<Map<String, Object>>(events.size());
		for (CloudTrailEvent cte : events) {
			records.add(flatten(cte.getEventData()));
		}

		Map<String, Object> document = new LinkedHashMap<String, Object>(1);
		document.put("Records", records);

		JSONValue.writeJSONString(document, arg0);
	}

	private Map<String, Object> flatten(CloudTrailDataStore ctds) {
		Map<String, Object> stuff = new LinkedHashMap<String, Object>();
		Map<String, Object> internalDataStore = null;
		try {
			internalDataStore = getDataStore(ctds);
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchFieldException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (Entry<String, Object> entry : internalDataStore.entrySet()) {
			Object value;
			if (entry.getValue() instanceof CloudTrailDataStore) {
				value = flatten((CloudTrailDataStore) entry.getValue());
			} else {
				value = entry.getValue();
			}
			stuff.put(entry.getKey(), value);
		}

		return stuff;
	}

	private Map<String, Object> getDataStore(CloudTrailDataStore ctds)
			throws IllegalAccessException, NoSuchFieldException, SecurityException {
		Field field = ctds.getClass().getSuperclass().getDeclaredField("dataStore");
		Object foo = FieldUtils.readField(field, ctds, true);
		return (Map<String, Object>) foo;
	}
}
