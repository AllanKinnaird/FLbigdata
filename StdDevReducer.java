import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class StdDevReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
   
    public void reduce(Text key, Iterator<DoubleWritable> values,
    		OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {    	

        ArrayList <Double> valuesCopy = new ArrayList<Double>();
        // We can use the values from the Iterator to calculate the mean but can't reset the Iterator
        //    so use an ArrayList to remember the values in order to calculate the Standard Deviation

        double sum = 0;
        long count = 0;        
        while (values.hasNext()) {
        	double value = values.next().get();
            sum += value;
            valuesCopy.add(value);
		    count++;
        }
                
        if (count > 0) {
	        double mean = sum / count;
	        
	        double varSum = 0;
	        // The sum of the squares of the deviations from the mean
	        for (double value : valuesCopy) {
	            varSum += (value - mean) * (value - mean);
	        }
	        
	        double sd = Math.sqrt(varSum / count);
	        	
		    output.collect(key, new DoubleWritable(sd));
		}

    }
}
