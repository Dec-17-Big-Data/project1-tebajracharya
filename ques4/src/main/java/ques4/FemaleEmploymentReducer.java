package ques4;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* Input is a Key:Value pair from the Mapper. 
* Output is Key:Value pair, and the key contains the Country and value is percentage of change in female employment since 2000.
*/

public class FemaleEmploymentReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	/**
	 * Reducer takes in the key value pair from the mapper and iterates over each key. During each iteration of key, the value string
	 * is split and stored in a String []. The input values are used to calculate the percentage change and the output is rounded off to 3 decimal points.
	 * The final percentage change is used as the value and the country is used for the key for the output.
	 */
	@Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text v: values) {
			String[] val = v.toString().split(",");	
			Double percentageChange = 0.0;
			percentageChange = Math.round((((Double.parseDouble(val[1]) - Double.parseDouble(val[0])) / Double.parseDouble(val[0])) * 1000.0))/1000.0;		
	        context.write(key, new DoubleWritable(percentageChange));
		}
    }
}
