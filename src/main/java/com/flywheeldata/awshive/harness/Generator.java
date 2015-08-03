package com.flywheeldata.awshive.harness;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.simple.JSONValue;

/**
 * A data generator, writes random records of a specified type to standard out.
 * Use the "--help" argument to print a help message listing arguments and
 * syntax.
 * 
 * @author ksitt_000
 *
 */
public class Generator {

	public static void main(String[] args) throws IOException, ParseException {
		Options opts = new Options();
		opts.addOption("t", "type", true,
				"The type of record to generate, currently supported options are: CloudTrails, DummyKV");
		opts.addOption("c", "count", true, "the number of records to generate");
		opts.addOption("h", "help", false, "print the help message");

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(opts, args);

		if (cmd.hasOption('h')) {
			HelpFormatter helpFormatter = new HelpFormatter();
			helpFormatter.printHelp("generator", opts);
			return;
		}

		String type = cmd.getOptionValue('t');
		int count = Integer.parseInt(cmd.getOptionValue("c"));

		Writer writer = new PrintWriter(System.out);

		switch (type) {
		case "CloudTrails":
			CloudTrailEventCollection ctec = new CloudTrailEventCollection();
			for (int n = 0; n < count; n++) {
				ctec.GenerateEvent();
			}
			JSONValue.writeJSONString(ctec, writer);
			break;
		case "DummyKV":
			KeyValue kv = new KeyValue();
			for (int n = 0; n < count; n++) {
				kv.addValue();
			}
			JSONValue.writeJSONString(kv, writer);
			break;
		default:
			break;
		}

		writer.close();
	}
}
