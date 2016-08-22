import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class AvgMaxTempMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	public void map(LongWritable key, Text value, OutputCollector<Text,
			DoubleWritable> output, Reporter reporter) throws IOException {
		
		// Set a couple of descriptive constants
		int MONTH = 3;
		int MAXTEMP = 5;

		//
		// Convert the line to a string
		//
		String line = value.toString();
		
		// Split it into its comma-delimited fields
		//
		String[] fields = line.split(",");
		
		try {
			double maxtemp = Double.parseDouble(fields[MAXTEMP]);
			// If we have a valid temperature, we also have a valid month
			// so return the month and temperature
			output.collect(new Text(fields[MONTH]), new DoubleWritable(maxtemp));
		}
		catch (ArrayIndexOutOfBoundsException e) {
			// There may be some entries without all their fields, so do nothing 
		}
		catch(NumberFormatException e) {
			// We expect some entries to be non-numeric, so do nothing
		}
		
	}
}
