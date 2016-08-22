import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class AvgMaxTempReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, 
			DoubleWritable> output, Reporter reporter) throws IOException {
	// Computes the mean of the values in the Iterator
	// Returns a key/value pair consisting of the key and the computed mean

		double sum = 0;
		long count = 0;

		while (values.hasNext()) {
			sum += values.next().get();
			count++;
		}

		if (count > 0) {
			output.collect(key, new DoubleWritable(sum / count));
		}

	}
}