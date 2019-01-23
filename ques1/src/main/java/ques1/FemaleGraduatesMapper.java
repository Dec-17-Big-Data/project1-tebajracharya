package ques1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
* Input is a comma-separated string, interpreted as Key:Value. 
* Output is Key:Value, and the key contains the Country and while the percentage of graduates is the value.
*/

public class FemaleGraduatesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	/**
	 * The map function gets a key which is a byte offset and value is a single line from the csv file.
	 * It uses a search criteria or indicator code to filter the data and then uses a regex to split the data into words
	 * which is then iterated from the end index to the front to get the most recent percentage value. The inputs are tested
	 * if it is a number or space. If it is a space, the exception is caught and ignored.
	 * If it is a number it is further tested to be below 30%. Once it is confirmed to be below 30%. The output country name and percentage
	 * is stored written back to the HDFS. This output is what we can retrieve to our local machines.
	 */
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (value.toString().contains("SE.TER.CMPL.FE.ZS")) {			
			String line[] = value.toString().split("\",\"?");
				for(int i = line.length - 1; i > 0 ; i--) {
					try{
						Double percent = Double.parseDouble(line[i]);
						if(percent < 30){	
							context.write(new Text(line[0].toString().substring(1)), new DoubleWritable(Math.round(percent * 1000.0)/1000.0));
							i = 0;
						}
					}
					catch(Exception e) {
						
					}
				}
		}
	}	
}