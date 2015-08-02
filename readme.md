AWS-HIVE
===========
Tools for interacting with Amazon Web Services data in Apache Hive

# Components
This library contains input formats which can be used to read AWS log data in a streaming, scalable way. There are no output format so this libary is only useful for reading external data. Since Hive does not support read-only external tables users must specify a dummy output format and are recommended to make the external directory containing their log files read-only. 

# Examples

Make sure to load the library before you get started:
```sql
ADD JAR /usr/lib/aws-hive-0.0.1-SNAPSHOT.jar;
```

Some examples make use of the [hive-json-serde](https://github.com/rcongiu/Hive-JSON-Serde) to parse the records, you'll need to load that as well if you want parsed data:
```sql
ADD JAR /usr/lib/json-serde-1.3.1-SNAPSHOT-jar-with-dependencies.jar;
```

## DDL

Create a simple table to read cloudtrails log files in the *user/hue/aws_logs/generated/cloudtrails* directory
```sql
CREATE EXTERNAL TABLE cloudtrails_raw
(record STRING)
STORED AS INPUTFORMAT 'com.flywheeldata.awshive.hadoop.CloudTrailsFileInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveNullValueSequenceFileOutputFormat'

LOCATION '/user/hue/aws_logs/generated/cloudtrails/';
```

This will cause Hive to present each CloudTrails event as an individual row containing one column - a JSON string representation of the entire event.

If you want parsed data you'll need to use the hive-json-serde and create your table as follows:
```sql
CREATE EXTERNAL TABLE cloudtrails_parsed
(eventTime STRING, eventVersion STRING, 
  userIdentity STRUCT<
    type:STRING, userName:STRING, principalId:STRING, arn:STRING, accountId:STRING, accessKeyId:STRING, 
    sessionContext:STRUCT<
      attributes:STRUCT<creationDate:STRING, mfaAuthenticated:STRING>,
      sessionIssuer:STRUCT<type:STRING, principalId:STRING, arn:STRING, accountId:STRING, userName:STRING>
    >,
    invokedBy:STRING, webIdFederationData:STRING
  >,
  eventSource STRING, eventName STRING, awsRegion STRING, sourceIPAddress STRING, userAgent STRING, errorCode STRING, errorMessage STRING, requestParameters MAP<STRING,STRING>, responseElements MAP<STRING,STRING>, requestID STRING, eventID STRING, eventType STRING, apiVersion STRING, recipientAccountID STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'com.flywheeldata.awshive.hadoop.CloudTrailsFileInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveNullValueSequenceFileOutputFormat'
LOCATION '/user/hue/aws_logs/generated/cloudtrails/';
```